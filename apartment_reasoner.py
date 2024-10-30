from knowledge_graph_creation.apartment_graph import ApartmentGraph
from geopy.geocoders import Nominatim

class ApartmentReasoner:

    def __init__(self, apartment_graph):
        self.apartment_graph = apartment_graph

    # Find the average price of apartments in each district
    def find_average_price_of_apartments_each_district(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH (a:Apartment)-[:LOCATED_IN]->(d:District)
                WHERE a.price IS NOT NULL
                RETURN d.name AS district, AVG(a.price) AS average_price
                ORDER BY average_price DESC;'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            return [record.data() for record in graph_response]

    # looks up addresses for apartments coordinates and adds new nodes
    def add_addresses(self):
        addresses = []
        query = f''' 
                USE {self.apartment_graph.db_name}
                MATCH (a:Apartment)
                WITH a.lon AS lon, a.lat AS lat
                RETURN lon, lat'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            for record in graph_response:
                if not {"lon":record["lon"], "lat":record["lat"]} in addresses:
                    addresses.append({"lon": record["lon"], "lat": record["lat"]})
        geolocator = Nominatim(user_agent="address_converter")

        for address in addresses:
            location = geolocator.reverse(f'''{address["lat"]}, {address["lon"]}''')
            address["name"] = location

        for address in addresses:
            query = f'''
                   USE {self.apartment_graph.db_name}
                   MERGE (ad:Address {{name: $name}})
                    ON CREATE SET a.lon = $lon, a.lat = $lat
                   '''
            with self.apartment_graph.driver.session() as session:
                session.run(query, name=address["name"], lon=address["lon"], lat=address["lat"])
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH(a: Apartment), (address:Address)
                WHERE a.lon = address.lon AND a.lat = address.lat
                MERGE(a) - [: LOCATED_AT_ADDRESS]->(address)'''
        with self.apartment_graph.driver.session() as session:
            session.run(query)

    # Add new relationship between apartments that have the same coordinates as neighbors
    def add_neighbors(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH (a1:Apartment), (a2:Apartment)
                WHERE a1 <> a2 AND a1.lon = a2.lon AND a1.lat = a2.lat
                MERGE (a1)-[:NEIGHBOR_OF]->(a2)'''

        with self.apartment_graph.driver.session() as session:
            return session.run(query)

    # Find the district with the most apartments
    def find_district_with_most_apartments(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH(a: Apartment)-[: LOCATED_IN]->(d:District)
                RETURN d.name AS district, COUNT(a) AS apartment_count
                ORDER BY apartment_count DESC'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            return [record.data() for record in graph_response]

    # Finding apartments with unusually high prices regarding the prices in the same district
    def find_expensive_apartments(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH (a:Apartment)-[:LOCATED_IN]->(d:District)
                WITH d, AVG(a.price) AS district_avg_price
                
                MATCH (a:Apartment)-[:LOCATED_IN]->(d)
                WHERE a.price > (district_avg_price * 3) 
                
                RETURN a, district_avg_price;'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            return [record.data() for record in graph_response]

    # Identifying potentially overcrowded districts
    def find_overcrowded_districts(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH (a:Apartment)-[:LOCATED_IN]->(d:District)
                WITH d, AVG(a.number_of_rooms) AS avgRooms

                MATCH (a:Apartment)-[:LOCATED_IN]->(d)
                WHERE avgRooms < 2.3

                RETURN DISTINCT d, avgRooms;'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            return [record.data() for record in graph_response]

    # Find the organisation that owns the most apartments
    def find_owner_with_most_apartments(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                MATCH (o:Owner)<-[:OWNED_BY]-(a:Apartment)
                WITH o, COUNT(a) AS apartmentCount
                ORDER BY apartmentCount DESC
                LIMIT 1
                RETURN o, apartmentCount;'''
        with self.apartment_graph.driver.session() as session:
            graph_response = session.run(query)
            return [record.data() for record in graph_response]

    def add_price_ranges(self):
        query = f'''
                USE {self.apartment_graph.db_name}
                UNWIND $price_ranges as price_range
                MERGE (p:PriceRange {{name: price_range[0]}})
                ON CREATE SET p.min_price = price_range[1][0], p.max_price = price_range[1][1]
                WITH price_range, p
                MATCH (a:Apartment)
                WHERE a.price >= price_range[1][0] AND a.price <= price_range[1][1]
                MERGE (a)-[:IN_PRICE_RANGE]->(p)'''
        with self.apartment_graph.driver.session() as session:
            return session.run(query, price_ranges=list(price_ranges.items()))

price_ranges = {
    "Low": (0, 400000),
    "Medium": (400001, 900000),
    "High": (900001, 1500000),
    "Very High": (1500001, 100000000)
}



if __name__ == "__main__":
    ag = ApartmentGraph("bolt://localhost:7687", "neo4j", "password", "neo4j")
    ar = ApartmentReasoner(ag)
    ar.add_neighbors()
    ar.add_price_ranges()

