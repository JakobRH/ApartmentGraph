from graphdatascience import GraphDataScience

class NodeRegressionModel:

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password", db_name="neo4j"):
        self.gds = GraphDataScience(uri, auth=(user, password))
        self.gds.set_database(db_name)
        self.clear()
        self.model_info = self.project()
        self.pipeline = self.create_pipeline()

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

    def create_pipeline(self):
        pipeline = self.gds.nr_pipe("regression_pipeline_apartments")
        pipeline.configureSplit(validationFolds=5, testFraction=0.2)
        pipeline.addLinearRegression(
            penalty=1e-5,
            patience=3,
            tolerance=1e-5,
            minEpochs=100,
            maxEpochs=500,
            learningRate={"range": [0.001, 1000]},
        )
        pipeline.configureAutoTuning(maxTrials=10)
        pipeline.addNodeProperty(
            "fastRP",
            embeddingDimension=256,
            propertyRatio=0.8,
            featureProperties=["floor", "number_of_rooms", "price", "quality", "size"],
            mutateProperty="frp_embedding",
            randomSeed=420,
        )
        pipeline.selectFeatures(["price", "frp_embedding"])
        return pipeline

    def train(self):
        model, train_result = self.pipeline.train(
            self.model_info,
            modelName="regression_apartment_model",
            targetNodeLabels=["Apartment"],
            targetProperty="price",
            metrics=["MEAN_SQUARED_ERROR", "MEAN_ABSOLUTE_ERROR", "ROOT_MEAN_SQUARED_ERROR"],
            randomSeed=420,
        )
        print("Model parameters: \n\t\t" + str(train_result["modelInfo"]["bestParameters"]))
        print("MEAN_SQUARED_ERROR      test score: " + str(
            train_result["modelInfo"]["metrics"]["MEAN_SQUARED_ERROR"]["test"]))
        print("MEAN_ABSOLUTE_ERROR     test score: " + str(
            train_result["modelInfo"]["metrics"]["MEAN_ABSOLUTE_ERROR"]["test"]))
        print("ROOT_MEAN_SQUARED_ERROR     test score: " + str(
            train_result["modelInfo"]["metrics"]["ROOT_MEAN_SQUARED_ERROR"]["test"]))

        predicted_targets_sample = model.predict_stream(self.model_info)

        predicted_targets_full = model.predict_stream(self.model_info)

        real_targets = self.gds.graph.nodeProperty.stream(self.model_info, "price")

        merged_full = real_targets.merge(predicted_targets_full, left_on="nodeId", right_on="nodeId")
        merged_all = merged_full.merge(predicted_targets_sample, left_on="nodeId", right_on="nodeId")

        print(merged_all.tail(10))

    def clear(self):
        self.gds.run_cypher("""
            CALL gds.beta.model.drop('regression_apartment_model-REG', False)
            YIELD modelInfo, loaded, shared, stored
            RETURN modelInfo.modelName AS modelName, loaded, shared, stored
        """)
        self.gds.run_cypher("""
            CALL gds.graph.drop('apartment-graph', False) YIELD graphName;
        """)
        self.gds.run_cypher("""
        CALL gds.beta.pipeline.drop('regression_pipeline_apartments', False)
        """)


if __name__ == '__main__':
    r = NodeRegressionModel()
    r.train()