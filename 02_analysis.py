"""
Step 2 — Analyse the Movie Knowledge Graph with Cypher.

Each query demonstrates a different graph traversal concept:
  Q1  Single-hop   — movies by a specific actor
  Q2  Edge filter  — person who both directed AND acted in the same movie
  Q3  Aggregation  — most-connected actors (degree centrality)
  Q4  Multi-hop    — people within 2 co-acting steps of a given actor

Run: python 02_analysis.py
"""

from neo4j import GraphDatabase
import config

URI      = config.NEO4J_URI
USERNAME = config.NEO4J_USER
PASSWORD = config.NEO4J_PASS


# ── Query functions ────────────────────────────────────────────────────────────

def q1_films_by_actor(session, actor_name: str = "Tom Hanks"):
    """
    Q1 — Single-hop traversal: Person → ACTED_IN → Movie
    Find all movies a specific actor appeared in, sorted by release year.
    """
    result = session.run(
        """
        MATCH (p:Person {name: $name})-[:ACTED_IN]->(m:Movie)
        RETURN m.title AS title, m.released AS year
        ORDER BY m.released
        """,
        name=actor_name,
    )
    print(f"\n── Q1  Films by '{actor_name}' ─────────────────────")
    for r in result:
        print(f"  {r['year']}  {r['title']}")


def q2_director_who_acted(session):
    """
    Q2 — Edge-type filtering: find people who DIRECTED and also ACTED_IN the same movie.
    This is a self-join on the same Person node via two different relationship types.
    """
    result = session.run(
        """
        MATCH (p:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(p)
        RETURN p.name AS person, m.title AS movie
        ORDER BY p.name
        """
    )
    print("\n── Q2  People who both directed and acted ────────────")
    for r in result:
        print(f"  {r['person']:25s}  →  {r['movie']}")


def q3_most_connected_actors(session, limit: int = 5):
    """
    Q3 — Degree centrality: count ACTED_IN relationships per person.
    The people with the most connections are the 'hubs' of the graph.
    """
    result = session.run(
        """
        MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
        RETURN p.name AS actor, count(m) AS film_count
        ORDER BY film_count DESC
        LIMIT $limit
        """,
        limit=limit,
    )
    print(f"\n── Q3  Top {limit} most-connected actors (degree centrality) ──")
    for i, r in enumerate(result, start=1):
        bar = "█" * r["film_count"]
        print(f"  {i}. {r['actor']:25s}  {bar}  ({r['film_count']} films)")


def q4_two_hop_coactors(session, start_actor: str = "Keanu Reeves"):
    """
    Q4 — Multi-hop traversal: walk up to 2 ACTED_IN edges.
    Starting from one actor, find everyone within 2 co-acting steps.
    This kind of query is very natural in Cypher but painful in SQL.
    """
    result = session.run(
        """
        MATCH (start:Person {name: $name})-[:ACTED_IN*1..2]->(m)<-[:ACTED_IN]-(coactor)
        WHERE coactor <> start
        RETURN DISTINCT coactor.name AS name
        ORDER BY name
        """,
        name=start_actor,
    )
    names = [r["name"] for r in result]
    print(f"\n── Q4  People within 2 co-acting steps of '{start_actor}' ──")
    print(f"  Found {len(names)} people:")
    # Print in three columns for readability
    for i in range(0, len(names), 3):
        row = names[i:i+3]
        print("  " + "   ".join(f"{n:<25}" for n in row))


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

    with driver.session() as session:
        q1_films_by_actor(session, "Tom Hanks")
        q2_director_who_acted(session)
        q3_most_connected_actors(session, limit=5)
        q4_two_hop_coactors(session, "Keanu Reeves")

    driver.close()
    print("\nAll queries completed.")


if __name__ == "__main__":
    main()