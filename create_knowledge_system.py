from neo4j import GraphDatabase
import pandas as pd

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687" #Add bolt connection here
NEO4J_USERNAME = "neo4j" #Add database username here
NEO4J_PASSWORD = "password" #Add database password here

# CSV file path
CSV_FILE_PATH = r"your\absolute\file\path"

# Batch size for processing
BATCH_SIZE = 500

# Connect to Neo4j
def connect_to_neo4j():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        print("Connected to Neo4j successfully.")
        return driver
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        raise

def create_graph(driver, df):
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1} with {len(batch)} records...")
            
            queries = []
            for _, row in batch.iterrows():
                pathogen_code = row['code']
                pathogen_name = row['species']

                for column in row.index[4:]:
                    antibiotic_name = column
                    susceptibility = row[column]

                    if susceptibility in ['S', 'R']:
                        relationship = "SUSCEPTIBLE" if susceptibility == 'S' else "RESISTANT"
                        queries.append((pathogen_code, pathogen_name, antibiotic_name, relationship))

            session.execute_write(create_nodes_and_relationships, queries)

def create_nodes_and_relationships(tx, queries):
    for pathogen_code, pathogen_name, antibiotic_name, relationship in queries:
        try:
            tx.run(
                """
                MERGE (p:Pathogen {code: $code, name: $name})
                MERGE (a:Antibiotic {name: $antibiotic})
                MERGE (p)-[r:""" + relationship + """]->(a)
                """,
                {
                    'code': pathogen_code,
                    'name': pathogen_name,
                    'antibiotic': antibiotic_name
                }
            )
        except Exception as e:
            print(f"Error during transaction: {e}")

def main():
    print("Loading CSV file...")
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"Loaded data with shape: {df.shape}")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    print("Connecting to Neo4j...")
    try:
        driver = connect_to_neo4j()
    except Exception as e:
        print("Failed to connect to Neo4j. Exiting...")
        return

    try:
        print("Creating graph...")
        create_graph(driver, df)
        print("Graph creation completed.")
    except Exception as e:
        print(f"Error during graph creation: {e}")
    finally:
        driver.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
