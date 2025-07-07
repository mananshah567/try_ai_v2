# Cosmos DB Gremlin Sync Version

from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
import pandas as pd
import time
import logging
from ipywidgets import IntProgress, VBox, HTML
from IPython.display import display

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------
# Config
# --------------------------------------
COSMOS_ENDPOINT = "wss://<your-account>.gremlin.cosmos.azure.com:443/"
DATABASE = "/dbs/<your-database>/colls/<your-graph>"
PRIMARY_KEY = "<your-primary-key>"
RU_LIMIT_PER_SECOND = 400.0

client = Client(
    COSMOS_ENDPOINT + DATABASE,
    'g',
    username=DATABASE,
    password=PRIMARY_KEY
)

# --------------------------------------
# RU Throttler
# --------------------------------------
class RUThrottler:
    def __init__(self, ru_limit_per_second):
        self.ru_limit = ru_limit_per_second
        self.window_start = time.time()
        self.ru_consumed = 0.0

    def throttle(self, ru):
        self.ru_consumed += ru
        elapsed = time.time() - self.window_start
        if elapsed < 1.0 and self.ru_consumed >= self.ru_limit:
            time.sleep(1.0 - elapsed)
            self.window_start = time.time()
            self.ru_consumed = 0.0
        elif elapsed >= 1.0:
            self.window_start = time.time()
            self.ru_consumed = 0.0

# --------------------------------------
# Property Escaper
# --------------------------------------
def escape_gremlin_str(s):
    return str(s).replace('\\', '\\\\').replace("'", "\\'")

# --------------------------------------
# Load Existing Vertices and Edges in Batches (with Progress)
# --------------------------------------
def load_existing_graph(client, throttler, batch_size=1000):
    inserted_vertex_ids = set()
    inserted_edge_keys = set()

    # Load vertices
    print("Loading existing vertices...")
    try:
        count_query = "g.V().count()"
        total_vertices = client.submit(count_query).all().result()[0]
    except Exception as e:
        logger.error(f"Could not fetch vertex count: {e}")
        total_vertices = 1

    vertex_progress = IntProgress(min=0, max=total_vertices, description='Vertices:')
    vertex_label = HTML()
    display(VBox([vertex_label, vertex_progress]))

    offset = 0
    vertex_start_time = time.time()
    while True:
        query = f"g.V().range({offset}, {offset + batch_size}).id()"
        try:
            result_set = client.submit(query)
            vertex_ids = result_set.all().result()
            if not vertex_ids:
                break
            inserted_vertex_ids.update(str(vid) for vid in vertex_ids)
            throttler.throttle(10)
            offset += batch_size

            vertex_progress.value = min(offset, total_vertices)
            elapsed = time.time() - vertex_start_time
            rate = vertex_progress.value / elapsed if elapsed > 0 else 0
            remaining = total_vertices - vertex_progress.value
            est_time = remaining / rate if rate > 0 else 0
            vertex_label.value = f"Loaded {vertex_progress.value} vertices so far... Estimated time left: {round(est_time, 1)}s"
        except Exception as e:
            logger.error(f"Failed to load vertices: {e}")
            break

    # Load edges
    print("Loading existing edges...")
    try:
        count_query = "g.E().count()"
        total_edges = client.submit(count_query).all().result()[0]
    except Exception as e:
        logger.error(f"Could not fetch edge count: {e}")
        total_edges = 1

    edge_progress = IntProgress(min=0, max=total_edges, description='Edges:')
    edge_label = HTML()
    display(VBox([edge_label, edge_progress]))

    offset = 0
    edge_start_time = time.time()
    while True:
        query = f"g.E().range({offset}, {offset + batch_size}).project('out', 'label', 'in').by(outV().id()).by(label()).by(inV().id())"
        try:
            result_set = client.submit(query)
            edges = result_set.all().result()
            if not edges:
                break
            for e in edges:
                edge_key = f"{e['out']}-{e['label']}->{e['in']}"
                inserted_edge_keys.add(edge_key)
            throttler.throttle(10)
            offset += batch_size

            edge_progress.value = min(offset, total_edges)
            elapsed = time.time() - edge_start_time
            rate = edge_progress.value / elapsed if elapsed > 0 else 0
            remaining = total_edges - edge_progress.value
            est_time = remaining / rate if rate > 0 else 0
            edge_label.value = f"Loaded {edge_progress.value} edges so far... Estimated time left: {round(est_time, 1)}s"
        except Exception as e:
            logger.error(f"Failed to load edges: {e}")
            break

    print("Finished loading existing graph.")
    return inserted_vertex_ids, inserted_edge_keys

# --------------------------------------
# Vertex Property Handlers
# --------------------------------------
def add_person_properties(row, query):
    return query + f".property('person name', '{escape_gremlin_str(row['name'])}')"

def add_school_properties(row, query):
    return query + f".property('school name', '{escape_gremlin_str(row['name'])}')"

def add_company_properties(row, query):
    return query + f".property('company name', '{escape_gremlin_str(row['name'])}')"

ENTITY_PROPERTY_DISPATCHER = {
    'person': add_person_properties,
    'school': add_school_properties,
    'company': add_company_properties,
}

# --------------------------------------
# Upload Vertices
# --------------------------------------
def upload_vertices(client, df_vertices, inserted_vertex_ids, throttler):
    progress = IntProgress(min=0, max=len(df_vertices), description='Vertices:')
    progress_label = HTML()
    display(VBox([progress_label, progress]))
    start_time = time.time()

    for i, row in df_vertices.iterrows():
        progress.value += 1
        vertex_id = str(row["ID"])
        label = str(row["Label"])
        if vertex_id in inserted_vertex_ids:
            continue

        query = f"g.addV('{label}').property('id', '{vertex_id}')"
        property_func = ENTITY_PROPERTY_DISPATCHER.get(label)
        if property_func:
            query = property_func(row, query)

        try:
            result_set = client.submit(query)
            result_set.all().result()
            inserted_vertex_ids.add(vertex_id)
            throttler.throttle(10)
        except GremlinServerError as e:
            logger.error(f"Vertex error {vertex_id}: {e}")

        elapsed = time.time() - start_time
        rate = progress.value / elapsed if elapsed > 0 else 0
        remaining = len(df_vertices) - progress.value
        est_time = remaining / rate if rate > 0 else 0
        progress_label.value = f"Inserted {progress.value}/{len(df_vertices)} vertices... Estimated time left: {round(est_time, 1)}s"

# --------------------------------------
# Upload Edges
# --------------------------------------
def upload_edges(client, df_edges, inserted_edge_keys, throttler):
    progress = IntProgress(min=0, max=len(df_edges), description='Edges:')
    progress_label = HTML()
    display(VBox([progress_label, progress]))
    start_time = time.time()

    for i, row in df_edges.iterrows():
        progress.value += 1
        out_id = str(row["OutV"])
        in_id = str(row["InV"])
        label = str(row["Label"])
        edge_id = f"{out_id}-{label}->{in_id}"

        if edge_id in inserted_edge_keys:
            continue

        query = f"g.V('{out_id}').addE('{label}').to(g.V('{in_id}'))"

        for col in df_edges.columns:
            if col in ["OutV", "InV", "Label"] or pd.isna(row[col]):
                continue
            val = row[col]
            if isinstance(val, str):
                query += f".property('{col}', '{escape_gremlin_str(val)}')"
            else:
                query += f".property('{col}', {val})"

        try:
            result_set = client.submit(query)
            result_set.all().result()
            inserted_edge_keys.add(edge_id)
            throttler.throttle(10)
        except GremlinServerError as e:
            logger.error(f"Edge error {edge_id}: {e}")

        elapsed = time.time() - start_time
        rate = progress.value / elapsed if elapsed > 0 else 0
        remaining = len(df_edges) - progress.value
        est_time = remaining / rate if rate > 0 else 0
        progress_label.value = f"Inserted {progress.value}/{len(df_edges)} edges... Estimated time left: {round(est_time, 1)}s"

# --------------------------------------
# Delete Vertices By ID List
# --------------------------------------
def delete_vertices_by_ids(client, vertex_ids, throttler):
    for vid in vertex_ids:
        try:
            query = f"g.V('{vid}').drop()"
            result_set = client.submit(query)
            result_set.all().result()
            throttler.throttle(10)
        except Exception as e:
            logger.error(f"Failed to delete vertex {vid}: {e}")

# --------------------------------------
# Delete Edges By ID List
# --------------------------------------
def delete_edges_by_ids(client, edge_ids, throttler):
    for eid in edge_ids:
        try:
            query = f"g.E('{eid}').drop()"
            result_set = client.submit(query)
            result_set.all().result()
            throttler.throttle(10)
        except Exception as e:
            logger.error(f"Failed to delete edge {eid}: {e}")

# --------------------------------------
# Delete All Vertices
# --------------------------------------
def delete_all_vertices_batched(client, throttler, batch_size=1000):
    print("Starting batched vertex deletion...")
    progress = IntProgress(description="Deleting vertices:")
    progress_label = HTML()
    display(VBox([progress_label, progress]))

    total_deleted = 0
    while True:
        try:
            query = f"g.V().limit({batch_size}).drop()"
            result_set = client.submit(query)
            result_set.all().result()
            throttler.throttle(10)
            total_deleted += batch_size
            progress.value = total_deleted
            progress_label.value = f"Deleted {total_deleted} vertices so far..."
        except Exception as e:
            if "NoSuchElementException" in str(e) or "script evaluation error" in str(e):
                progress_label.value = "No more vertices to delete."
                break
            else:
                print(f"Error during deletion: {e}")
                break
    print("Finished deleting all vertices.")

# --------------------------------------
# Delete All Edges
# --------------------------------------
def delete_all_edges_batched(client, throttler, batch_size=1000):
    print("Starting batched edge deletion...")
    progress = IntProgress(description="Deleting edges:")
    progress_label = HTML()
    display(VBox([progress_label, progress]))

    total_deleted = 0
    while True:
        try:
            query = f"g.E().limit({batch_size}).drop()"
            result_set = client.submit(query)
            result_set.all().result()
            throttler.throttle(10)
            total_deleted += batch_size
            progress.value = total_deleted
            progress_label.value = f"Deleted {total_deleted} edges so far..."
        except Exception as e:
            if "NoSuchElementException" in str(e) or "script evaluation error" in str(e):
                progress_label.value = "No more edges to delete."
                break
            else:
                print(f"Error during deletion: {e}")
                break
    print("Finished deleting all edges.")

# --------------------------------------
# Clear Entire Graph
# --------------------------------------
def clear_graph(client, throttler):
    print("Clearing entire graph (edges then vertices)...")
    delete_all_edges_batched(client, throttler)
    delete_all_vertices_batched(client, throttler)
    print("Graph cleared.")
