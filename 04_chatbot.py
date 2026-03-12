import os
import json
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
from openai import OpenAI
import ollama
from dotenv import load_dotenv

import config
import importlib.util

# Helper to import numeric modules (e.g., 05_naive_rag.py)
def dynamic_import(module_name, path):
    if not os.path.exists(path):
        return None
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

naive_rag_mod = dynamic_import("naive_rag", "03_naive_rag.py")

# --- Config ---
NEO4J_URI = config.NEO4J_URI
NEO4J_USER = config.NEO4J_USER
NEO4J_PASS = config.NEO4J_PASS

# --- LLM Abstraction ---

class LLMProvider(ABC):
    @abstractmethod
    def chat(self, prompt: str, system_message: str = "", response_format: str = "text") -> str:
        """Abstract method for LLM chat."""
        ...

class OpenAIProvider(LLMProvider):
    def __init__(self, model="gpt-4o-mini"):
        self.client = OpenAI()
        self.model = model
    
    def chat(self, prompt: str, system_message: str = "", response_format: str = "text") -> str:
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})
        
        extra_args = {}
        if response_format == "json":
            extra_args["response_format"] = {"type": "json_object"}
            
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            **extra_args
        )
        return response.choices[0].message.content

class OllamaProvider(LLMProvider):
    def __init__(self, model="llama3"):
        self.model = model
    
    def chat(self, prompt: str, system_message: str = "", response_format: str = "text") -> str:
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})
        
        format_arg = 'json' if response_format == 'json' else None
        
        response = ollama.chat(
            model=self.model,
            messages=messages,
            format=format_arg
        )
        return response['message']['content']

# --- Graph Engine ---

class GraphRAGEngine:
    def __init__(self, provider: LLMProvider):
        self.provider = provider
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        # Load Naive RAG engine dynamically
        self.naive_engine = None
        if naive_rag_mod:
            self.naive_engine = naive_rag_mod.NaiveRAGEngine()
    
    def get_schema(self) -> Dict[str, Any]:
        """Fetch a structured schema for the LLM."""
        with self.driver.session() as session:
            # Node labels and properties
            node_props = session.run("""
                CALL db.schema.nodeTypeProperties() 
                YIELD nodeLabels, propertyName
                RETURN nodeLabels[0] as label, collect(propertyName) as props
            """)
            nodes = {record["label"]: record["props"] for record in node_props}
            
            # Relationships and their strict directions
            rel_patterns = session.run("""
                CALL db.schema.visualization() 
                YIELD relationships 
                UNWIND relationships as rel
                RETURN startNode(rel).name as start, type(rel) as type, endNode(rel).name as end
            """)
            rels = [{"start": r['start'], "type": r['type'], "end": r['end']} for r in rel_patterns]
            
            return {"nodes": nodes, "relationships": rels}

    def query(self, user_question: str) -> Dict[str, str]:
        """Runs both Naive RAG and Graph RAG for comparison."""
        
        # 1. Run Naive RAG (if module exists)
        naive_answer = "Naive RAG module (05_naive_rag.py) not found."
        if self.naive_engine:
            naive_answer = self.naive_engine.query(user_question)
        
        # 2. Run Graph RAG
        schema = self.get_schema()
        
        # --- Stage 1: Intent Extraction ---
        intent_system_msg = """Identify the user's intent for a Movie Knowledge Graph. 
        Return ONLY valid JSON.
        
        Intents:
        - find_coactors: Use ONLY for simple co-stars questions. (Params: actor, movie)
        - actor_movie_count: Use for counting movies of an actor. (Params: actor)
        - find_director: Use for finding a director. (Params: movie)
        - directed_and_acted: Use for directors who acted. (Params: None)
        - k_hop_coactors: Use if the question contains "co-actors of co-actors", "co-stars of co-stars", "2-hop", or any double-layered relationship. This is higher priority than find_coactors. (Params: actor)
        - frequently_acted: Use for actors with MORE than X movies. (Params: count)
        - other: Default.
        
        Example: {"intent": "find_coactors", "params": {"actor": "Keanu Reeves", "movie": "The Matrix"}}
        Example: {"intent": "k_hop_coactors", "params": {"actor": "Keanu Reeves"}} // Use this for "co-actors of co-actors"
        """
        
        cypher_query = None
        data = []
        try:
            extraction_json = self.provider.chat(f"Question: {user_question}", system_message=intent_system_msg, response_format="json")
            extraction = json.loads(extraction_json)
            intent = extraction.get("intent")
            params = extraction.get("params", {})
            
            # --- Stage 2: Template Matching ---
            if intent == "find_coactors" and "actor" in params:
                if "movie" in params:
                    cypher_query = f"MATCH (p:Person {{name: '{params['actor']}'}})-[:ACTED_IN]->(m:Movie {{title: '{params['movie']}'}})<-[:ACTED_IN]-(coactor) RETURN DISTINCT coactor.name"
                else:
                    cypher_query = f"MATCH (p:Person {{name: '{params['actor']}'}})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(coactor) RETURN DISTINCT coactor.name"
            elif intent == "actor_movie_count" and "actor" in params:
                cypher_query = f"MATCH (p:Person {{name: '{params['actor']}'}})-[:ACTED_IN]->(m:Movie) RETURN count(m) AS count"
            elif intent == "find_director" and "movie" in params:
                cypher_query = f"MATCH (m:Movie {{title: '{params['movie']}'}})<-[:DIRECTED]-(d:Person) RETURN d.name"
            elif intent == "directed_and_acted":
                cypher_query = "MATCH (p:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(p) RETURN p.name, m.title"
            elif intent == "k_hop_coactors" and "actor" in params:
                cypher_query = f"MATCH (start:Person {{name: '{params['actor']}'}})-[:ACTED_IN]->()<-[:ACTED_IN]-(co1)-[:ACTED_IN]->()<-[:ACTED_IN]-(co2) WHERE co2 <> start AND co2 <> co1 RETURN DISTINCT co2.name"
            elif intent == "frequently_acted" and "count" in params:
                cypher_query = f"MATCH (p:Person)-[:ACTED_IN]->(m:Movie) WITH p, count(m) AS movieCount WHERE movieCount > {params['count']} RETURN p.name, movieCount"
            
        except Exception as e:
            pass

        # --- Stage 3: Execution / Fallback ---
        if cypher_query:
            print(f"--- Template Match Found ---\n{cypher_query}\n---------------------------")
            try:
                with self.driver.session() as session:
                    result = session.run(cypher_query)
                    data = [list(record.values())[0] for record in result]
            except Exception as e:
                cypher_query = None

        if not cypher_query:
            schema_text = "NODE LABELS & PROPERTIES:\n"
            for label, props in schema["nodes"].items():
                schema_text += f"- {label}: {props}\n"
            schema_text += "\nSTRICT RELATIONSHIPS:\n"
            for rel in schema["relationships"]:
                schema_text += f"- ({rel['start']})-[:{rel['type']}]->({rel['end']})\n"

            fallback_system_msg = f"""You are a Neo4j Cypher expert.
            STRICT RULES:
            1. RETURN ONLY THE CYPHER QUERY. NO EXPLANATION. NO MARKDOWN.
            2. Use ONLY standard arrow syntax: (p:Person {{name: "Name"}})-[:ACTED_IN]->(m:Movie)
            3. ALWAYS use parentheses for nodes: (node)
            4. Use valid directional arrows: (a)-[:TYPE]->(b) or (a)<-[:TYPE]-(b).
            
            SCHEMA:
            {schema_text}
            
            TEMPLATE: 
            MATCH (p:Person {{name: "Name"}})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(co:Person) RETURN DISTINCT co.name
            """
            
            cypher_query = self.provider.chat(f"Question: {user_question}\nCypher:", system_message=fallback_system_msg)
            # Extra cleaning to remove ANY chatty text the 3B model might add
            cypher_query = cypher_query.strip().split('\n')[-1] # Usually the code is at the end if it's chatty
            if "MATCH" not in cypher_query.upper(): # Try safe fallback if end-line is not cypher
                cypher_query = cypher_query.strip().split('\n')[0]
            
            cypher_query = cypher_query.strip().strip('`').replace("```cypher", "").replace("```", "").split(';')[0]
            print(f"--- Graph RAG Cypher ---\n{cypher_query}\n------------------------------")
            
            try:
                with self.driver.session() as session:
                    result = session.run(cypher_query)
                    # For fallback, results can be complex dictionaries
                    data = [dict(record) for record in result]
            except Exception as e:
                graph_answer = f"Error: {e}"
                return {"naive": naive_answer, "graph": graph_answer}

        if not data:
            graph_answer = "I couldn't find any information in the database."
        else:
            # If the result is a simple list of strings (names), return it directly
            # to prevent small models (3B) from hallucinating similar names (e.g., Tom Cruise vs Tom Hanks).
            if isinstance(data, list) and all(isinstance(i, str) for i in data):
                graph_answer = ", ".join(data)
            else:
                if isinstance(data, list):
                    data_str = ", ".join([str(d) for d in data])
                else:
                    data_str = str(data)

                answer_prompt = f"""
                Question: {user_question}
                Database Result: {data_str}
                
                Rule: 
                1. If 'Database Result' is NOT empty, simply list the names/data found in it.
                2. Do NOT say "I don't know". 
                3. Do NOT explain. 
                
                Answer:
                """
                graph_answer = self.provider.chat(answer_prompt)

        return {"naive": naive_answer, "graph": graph_answer}

    def close(self):
        self.driver.close()

# --- Main ---

def main():
    print("=" * 60)
    print("  Comparison Lab: Naive RAG vs Graph RAG")
    print("=" * 60)
    
    provider_type = config.LLM_PROVIDER
    if provider_type == "ollama":
        provider = OllamaProvider(model=config.LLM_MODEL)
    else:
        provider = OpenAIProvider(model=config.LLM_MODEL)
    
    engine = GraphRAGEngine(provider)
    
    print(f"Running with provider: {provider_type.upper()}")
    print("Type 'exit' to quit.")
    
    print("\nSuggested Questions:")
    for sq in config.SUGGESTED_QUESTIONS:
        print(f"  - {sq}")
    
    while True:
        try:
            question = input("\nYou (or type a suggested question): ").strip()
        except (EOFError, KeyboardInterrupt):
            break
            
        if not question: continue
        if question.lower() in ("exit", "quit", "q"): break
        
        results = engine.query(question)
        
        print("\n" + "-"*30 + " NAIVE RAG " + "-"*30)
        print(results['naive'])
        print("-" * 71)
        
        print("\n" + "-"*30 + " GRAPH RAG " + "-"*30)
        print(results['graph'])
        print("-" * 71)
    
    engine.close()
    print("\nGoodbye!")

if __name__ == "__main__":
    main()