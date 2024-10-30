from pykeen.pipeline import pipeline
from matplotlib import pyplot as plt
from knowledge_graph_creation.apartment_graph import ApartmentGraph
from pykeen.triples import TriplesFactory
from pykeen.predict import predict_target

class ApartmentEmbedding:

    def __init__(self, apartment_graph):
        self.model = None
        self.apartment_graph = apartment_graph
        self.triples_factory = None

    def train(self):
        data = self.apartment_graph.get_data_for_embedding()
        self.triples_factory = TriplesFactory.from_labeled_triples(
            triples=data[['subject', 'predicate', 'object']].values,
        )
        training, testing, validation = self.triples_factory.split([.8, .1, .1])
        result = pipeline(
            model='Rotate',
            loss="BCEWithLogitsLoss",
            training=training,
            testing=testing,
            validation=validation,
            model_kwargs=dict(embedding_dim=50),
            optimizer_kwargs=dict(lr=0.001),
            training_kwargs=dict(num_epochs=50, use_tqdm_batch=False, batch_size=32),
        )
        self.model = result.model
        plt.plot(result.losses)
        plt.xlabel("Epoch")
        plt.ylabel("Loss")
        plt.title('RotatE')
        plt.savefig('rotate_embedding_2.png')

    def predict(self, subject, relation):
        return predict_target(self.model, head=subject, relation=relation,
                              triples_factory=self.triples_factory)


if __name__ == "__main__":
    ag = ApartmentGraph("bolt://localhost:7687", "neo4j", "password", "neo4j")
    ae = ApartmentEmbedding(ag)
    ae.train()
