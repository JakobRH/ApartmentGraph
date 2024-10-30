from graphdatascience import GraphDataScience
from matplotlib import pyplot as plt


class SageModel:

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password", db_name="neo4j"):
        self.gds = GraphDataScience(uri, auth=(user, password))
        self.gds.set_database(db_name)
        self.clear()
        self.model_info = self.project()

    def project(self):
        model_info, _ = self.gds.graph.project(
            "apartment-graph",
            [{"Apartment": {"properties": ["floor", "number_of_rooms", "price", "quality", "size"]}},
             "Owner", "District"],
            [{"LOCATED_IN": {"orientation": "UNDIRECTED", "properties": []}},
             {"OWNED_BY": {"orientation": "UNDIRECTED", "properties": []}},
             {"NEIGHBOR_OF": {"orientation": "UNDIRECTED", "properties": []}}]
        )
        return model_info

    def train(self):
        model, train_result = self.gds.beta.graphSage.train(
            self.model_info,
            modelName="sage_apartment_model",
            featureProperties=["floor", "number_of_rooms", "price", "quality", "size"],
            randomSeed=420,
            embeddingDimension=64,
            projectedFeatureDimension=64,
            activationFunction='sigmoid',
            maxIterations=20,
            searchDepth=10,
            learningRate=0.001,
            penaltyL2=1e-5,
            tolerance=0,
            epochs=100)
        print(train_result)
        for values in train_result:
            print(values)
        print(train_result['modelInfo']['metrics']['epochLosses'])
        plt.plot(train_result['modelInfo']['metrics']['epochLosses'])
        plt.xlabel('Epoch')
        plt.ylabel('Loss')
        plt.title('Training Loss over Epochs for SAGE')
        plt.savefig('epoch_losses_sage2.png')
        self.gds.beta.graphSage.write(self.model_info, modelName="sage_apartment_model", writeProperty="sage_embeddings")

    def clear(self):
        self.gds.run_cypher("""
                    CALL gds.beta.model.drop('sage_apartment_model', False)
                    YIELD modelInfo, loaded, shared, stored
                    RETURN modelInfo.modelName AS modelName, loaded, shared, stored
                """)
        self.gds.run_cypher("""
                    CALL gds.graph.drop('apartment-graph', False) YIELD graphName;
                """)

    def get_similar_apartments(self, apartment_id):
        result = self.gds.run_cypher(f"""
        MATCH(a1: Apartment)
        MATCH(a2: Apartment)
        WHERE a1.id <> a2.id and a1.id = '{apartment_id}'
        WITH a1, a2, gds.similarity.euclideanDistance(a1.sage_embeddings, a2.sage_embeddings) as distance
        RETURN  a1.id, a2.id, distance
        Order by distance
        limit 5""")
        print(result)

    def get_similar_owners(self):
        result = self.gds.run_cypher(f"""
        MATCH(o1: Owner)
        MATCH(o2: Owner)
        WHERE o1.name <> o2.name
        WITH o1, o2, gds.similarity.euclideanDistance(o1.sage_embeddings, o2.sage_embeddings) as distance
        RETURN  o1.name, o2.name, distance
        Order by distance
        limit 5""")
        print(result)

    def get_similar_districts(self):
        result = self.gds.run_cypher(f"""
        MATCH(d1: District)
        MATCH(d2: District)
        WHERE d1.name <> d2.name
        WITH d1, d2, gds.similarity.euclideanDistance(d1.sage_embeddings, d2.sage_embeddings) as distance
        RETURN  d1.name, d2.name, distance
        Order by distance
        limit 5""")
        print(result)


if __name__ == '__main__':
    r = SageModel()
    r.train()
    r.get_similar_apartments(666356897)
    r.get_similar_owners()
    r.get_similar_districts()
