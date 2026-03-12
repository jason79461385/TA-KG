"""
Step 1 — Load the Movie dataset and verify the connection.

The built-in Neo4j Movie dataset contains:
  - 38  Movie nodes  (title, released, tagline)
  - 133 Person nodes (name, born)
  - 253 Relationships (ACTED_IN, DIRECTED, WROTE, PRODUCED, FOLLOWS, REVIEWED)

Run: python 01_load_and_verify.py
"""

from neo4j import GraphDatabase

# ── Connection settings ────────────────────────────────────────────────────────
URI      = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password123"


def load_movie_dataset(session):
    """Use Neo4j's built-in CALL to load the classic Movie dataset."""
    # This Cypher call populates the database with the Movie sample graph.
    session.run("CALL db.schema.visualization()")     # warm up
    session.run("""
        CALL apoc.load.json(
            'https://raw.githubusercontent.com/neo4j-graph-examples/movies/main/data/movies.json'
        ) YIELD value
        CALL apoc.create.addLabels(value.nodes, value.labels) YIELD node
        RETURN count(node)
    """)


def load_movie_dataset_native(session):
    """
    Load the Movie dataset with a consistent signature and a Bridge for multi-hop demos.
    """
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

        // THE BRIDGE: Hugo Weaving also in Green Mile
        MERGE (Hugo)-[:ACTED_IN]->(TheGreenMile)

        MERGE (CastAway:Movie {title:'Cast Away', released:2000})
        MERGE (TomH)-[:ACTED_IN]->(CastAway)
        MERGE (RobertZ:Person {name:'Robert Zemeckis'})
        MERGE (RobertZ)-[:DIRECTED]->(CastAway)
    """)


def verify(session):
    """Count nodes and relationships to confirm the dataset loaded correctly."""
    result = session.run("""
        MATCH (n)
        RETURN labels(n)[0] AS label, count(n) AS count
        ORDER BY count DESC
    """)
    print("\n── Node counts ────────────────────────────")
    for record in result:
        print(f"  {record['label']:15s}  {record['count']}")

    result = session.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel_type, count(r) AS count
        ORDER BY count DESC
    """)
    print("\n── Relationship counts ────────────────────")
    for record in result:
        print(f"  {record['rel_type']:15s}  {record['count']}")


def main():
    print("Connecting to Neo4j at", URI)
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    with driver.session() as session:
        # Test connection
        result = session.run("RETURN 'Connected!' AS msg")
        print(result.single()["msg"])

        # Load the Movie dataset (using pure Cypher, no APOC needed)
        print("\nLoading Movie dataset...")
        load_movie_dataset_native(session)
        print("Dataset loaded successfully.")

        # Verify counts
        verify(session)

    driver.close()
    print("\nDone. Open http://localhost:7474 to explore in the browser.")


if __name__ == "__main__":
    main()