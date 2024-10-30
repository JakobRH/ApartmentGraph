import json
import pandas as pd
from neo4j import GraphDatabase


class ApartmentGraph:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password", db_name="neo4j"):
        print("Connecting to Neo4j")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.db_name = db_name

    def close(self):
        self.driver.close()

    def import_json(self, json_file):
        with self.driver.session() as session:
            session.execute_write(self.create_districts)
        with open(json_file, 'r') as json_file:
            data = json.load(json_file)
            for apartment in data:
                self.create_apartment_data(apartment)

    def create_apartment_data(self, apartment):
        with self.driver.session() as session:
            if not "location_quality" in apartment or type(apartment["location_quality"]) != int:
                return
            if not "price" in apartment or type(apartment["price"]) == str:
                return
            else:
                apartment["price"] = int(apartment["price"])
            if not "floor" in apartment or type(apartment["floor"]) == str:
                return
            else:
                apartment["floor"] = int(apartment["floor"])
            if not "lon" in apartment:
                apartment["lon"] = None
            else:
                apartment["lon"] = float(apartment["lon"])
            if not "lat" in apartment:
                apartment["lat"] = None
            else:
                apartment["lon"] = float(apartment["lat"])
            if not "estate_size" in apartment:
                return
            if not "number_of_rooms" in apartment:
                return

            if "orgname" in apartment:
                session.execute_write(self.create_update_apartment_owner, apartment["orgname"])
                session.execute_write(self.create_apartment, apartment)
            else:
                session.execute_write(self.create_apartment_without_owner, apartment)

    def create_apartment(self, tx, apartment):
        query = f'''
                USE {self.db_name}
                MATCH (o:Owner {{name: $owner_name}})   
                MATCH (d:District {{postal_code: $postal_code}})
                
                MERGE (a:Apartment {{id: $apartment_id}})
                ON CREATE SET a.price = $price, a.floor = $floor, a.lon = $lon, a.lat = $lat, a.quality = $quality, 
                a.size = $size, a.number_of_rooms = $number_of_rooms
                
                MERGE (a)-[r1:OWNED_BY]->(o)
                MERGE (a)-[r2:LOCATED_IN]->(d)
                '''
        tx.run(query, owner_name=apartment["orgname"], postal_code=apartment["postcode"], apartment_id=apartment["id"]
               , price=apartment["price"], lon=apartment["lon"], lat=apartment["lat"],
               quality=apartment["location_quality"],
               size=apartment["estate_size"], number_of_rooms=apartment["number_of_rooms"], floor=apartment["floor"])

    def create_apartment_without_owner(self, tx, apartment):
        query = f'''
                USE {self.db_name}
                MATCH (d:District {{postal_code: $postal_code}})
                MERGE (a:Apartment {{id: $apartment_id}})
                ON CREATE SET a.price = $price, a.floor = $floor, a.lon = $lon, a.lat = $lat, a.quality = $quality, 
                a.size = $size, a.number_of_rooms = $number_of_rooms
        
                MERGE (a)-[r2:LOCATED_IN]->(d)
                '''
        tx.run(query, postal_code=apartment["postcode"], apartment_id=apartment["id"]
               , price=apartment["price"], lon=apartment["lon"], lat=apartment["lat"],
               quality=apartment["location_quality"],
               size=apartment["estate_size"], number_of_rooms=apartment["number_of_rooms"], floor=apartment["floor"])

    def create_districts(self, tx):
        vienna_districts = [
            {"postal_code": 1010, "name": "Innere Stadt"},
            {"postal_code": 1020, "name": "Leopoldstadt"},
            {"postal_code": 1030, "name": "Landstraße"},
            {"postal_code": 1040, "name": "Wieden"},
            {"postal_code": 1050, "name": "Margareten"},
            {"postal_code": 1060, "name": "Mariahilf"},
            {"postal_code": 1070, "name": "Neubau"},
            {"postal_code": 1080, "name": "Josefstadt"},
            {"postal_code": 1090, "name": "Alsergrund"},
            {"postal_code": 1100, "name": "Favoriten"},
            {"postal_code": 1110, "name": "Simmering"},
            {"postal_code": 1120, "name": "Meidling"},
            {"postal_code": 1130, "name": "Hietzing"},
            {"postal_code": 1140, "name": "Penzing"},
            {"postal_code": 1150, "name": "Rudolfsheim-Fünfhaus"},
            {"postal_code": 1160, "name": "Ottakring"},
            {"postal_code": 1170, "name": "Hernals"},
            {"postal_code": 1180, "name": "Währing"},
            {"postal_code": 1190, "name": "Döbling"},
            {"postal_code": 1200, "name": "Brigittenau"},
            {"postal_code": 1210, "name": "Floridsdorf"},
            {"postal_code": 1220, "name": "Donaustadt"},
            {"postal_code": 1230, "name": "Liesing"}]
        for district in vienna_districts:
            query = f'''
                    USE {self.db_name}
                    MERGE (d:District {{postal_code: $postal_code}})
                    ON CREATE SET d.name = $name
                    '''
            tx.run(query, postal_code=district["postal_code"], name=district["name"])

    def create_update_apartment_owner(self, tx, name):
        query = f'''
                USE {self.db_name}
                MERGE (o:Owner {{name: $name}})
                '''
        tx.run(query, name=name)

    def clear_db(self):
        with self.driver.session() as session:
            session.run(f"USE {self.db_name} MATCH (n) DETACH DELETE n")

    def get_data_for_embedding(self, file_path=""):
        result = []
        query = f'''
                USE {self.db_name}
                MATCH (a:Apartment)
                MATCH (a)-[r:LOCATED_IN]->(d:District)
                RETURN a.id AS subject, "LOCATED_IN" AS predicate, d.postal_code AS object;'''
        with self.driver.session() as session:
            graph_response = session.run(query)
            result = pd.DataFrame([r.values() for r in graph_response], columns=graph_response.keys())
        query = f'''
                USE {self.db_name}
                MATCH (a:Apartment)
                MATCH (a)-[r:IN_PRICE_RANGE]->(p:PriceRange)
                RETURN a.id AS subject, "IN_PRICE_RANGE" AS predicate, p.name AS object;'''
        with self.driver.session() as session:
            graph_response = session.run(query)
            temp = pd.DataFrame([r.values() for r in graph_response], columns=graph_response.keys())
        result = result.append(temp, ignore_index=True)
        query = f'''
                        USE {self.db_name}
                        MATCH (a:Apartment)
                        MATCH (a)-[r:OWNED_BY]->(o:Owner)
                        RETURN a.id AS subject, "OWNED_BY" AS predicate, o.name AS object;'''
        with self.driver.session() as session:
            graph_response = session.run(query)
            temp = pd.DataFrame([r.values() for r in graph_response], columns=graph_response.keys())
        result = result.append(temp, ignore_index=True)
        result["subject"] = result["subject"].astype(str)
        result["object"] = result["object"].astype(str)
        return result


if __name__ == "__main__":
    ag = ApartmentGraph()
    ag.clear_db()
    ag.import_json("./result_for_db.json")
    ag.close()
