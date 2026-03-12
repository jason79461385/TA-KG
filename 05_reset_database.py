from neo4j import GraphDatabase
import config

# Use config.py
URI      = config.NEO4J_URI
USERNAME = config.NEO4J_USER
PASSWORD = config.NEO4J_PASS

def reset_and_load():
    print(f"Connecting to Neo4j at {URI}...")
    try:
        driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
        with driver.session() as session:
            print("Wiping database (DETACH DELETE)...")
            session.run("MATCH (n) DETACH DELETE n")
            
            print("Reloading baseline Movie dataset with educational Bridge...")
            session.run("""
                MERGE (TheMatrix:Movie {title:'The Matrix', released:1999})
                MERGE (Keanu:Person {name:'Keanu Reeves', born:1964})
                MERGE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})
                MERGE (Laurence:Person {name:'Laurence Fishburne', born:1961})
                MERGE (Hugo:Person {name:'Hugo Weaving', born:1960})
                
                MERGE (Keanu)-[:ACTED_IN]->(TheMatrix)
                MERGE (Carrie)-[:ACTED_IN]->(TheMatrix)
                MERGE (Laurence)-[:ACTED_IN]->(TheMatrix)
                MERGE (Hugo)-[:ACTED_IN]->(TheMatrix)

                MERGE (TomH:Person {name:'Tom Hanks', born:1956})
                MERGE (TheGreenMile:Movie {title:'The Green Mile', released:1999})
                MERGE (TomH)-[:ACTED_IN]->(TheGreenMile)
                
                MERGE (ForrestGump:Movie {title:'Forrest Gump', released:1994})
                MERGE (TomH)-[:ACTED_IN]->(ForrestGump)

                MERGE (YouveGotMail:Movie {title:"You've Got Mail", released:1998})
                MERGE (TomH)-[:ACTED_IN]->(YouveGotMail)

                // THE BRIDGE
                MERGE (Hugo)-[:ACTED_IN]->(TheGreenMile)

                MERGE (CastAway:Movie {title:'Cast Away', released:2000})
                MERGE (TomH)-[:ACTED_IN]->(CastAway)
                MERGE (RobertZ:Person {name:'Robert Zemeckis'})
                MERGE (RobertZ)-[:DIRECTED]->(CastAway)
            """)
            
            # Verify
            result = session.run("MATCH (n) RETURN count(n) AS count")
            print(f"Database reset complete. Total nodes: {result.single()['count']}")
        driver.close()
    except Exception as e:
        print(f"Error during reset: {e}")

if __name__ == "__main__":
    reset_and_load()
