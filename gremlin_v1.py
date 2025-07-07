# Cosmos DB Gremlin Sync Version (Refined)

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
        self.total_ru = 0.0
        self.total_ops = 0

    def throttle(self, ru):
        self.ru_consumed += ru
        self.total_ru += ru
        self.total_ops += 1
        print(f"Throttling after operation {self.total_ops}: +{ru} RU, total this second = {self.ru_consumed:.2f}, total = {self.total_ru:.2f} RU")

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
# Load Existing Vertices and Edges (Batched with Progress)
# --------------------------------------
def load_existing_graph(client, throttler, batch_size=1000):
    inserted_vertex_ids = set()
    inserted_edge_keys = set()

    print("Loading existing vertices...")
    vertex_progress = IntProgress(min=0, description='Vertices:')
    vertex_label = HTML()
    display(VBox([vertex_label, vertex_progress]))

    offset = 0
    while True:
        query = f"g.V().range({offset}, {offset + batch_size}).id()"
        try:
            result_set = client.submit(query)
            vertex_ids = result_set.all().result()
            if not vertex_ids:
                break
            inserted_vertex_ids.update(str(vid) for vid in vertex_ids)
            offset += batch_size
            vertex_progress.value = offset
            elapsed = time.time() - throttler.window_start
            est_total = (elapsed / offset * len(inserted_vertex_ids)) if offset else 0
            remaining = est_total - elapsed if est_total > elapsed else 0
            vertex_label.value = f"Loaded {offset} vertices so far... Estimated time left: {remaining:.1f}s"
            throttler.throttle(10)
        except Exception as e:
            logger.error(f"Failed to load vertices: {e}")
            break

    print("Loading existing edges...")
    edge_progress = IntProgress(min=0, description='Edges:')
    edge_label = HTML()
    display(VBox([edge_label, edge_progress]))

    offset = 0
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
            offset += batch_size
            edge_progress.value = offset
            elapsed = time.time() - throttler.window_start
            est_total = (elapsed / offset * len(inserted_edge_keys)) if offset else 0
            remaining = est_total - elapsed if est_total > elapsed else 0
            edge_label.value = f"Loaded {offset} edges so far... Estimated time left: {remaining:.1f}s"
            throttler.throttle(10)
        except Exception as e:
            logger.error(f"Failed to load edges: {e}")
            break

    print(f"Finished loading existing graph. Total vertices: {len(inserted_vertex_ids)}, Total edges: {len(inserted_edge_keys)}")
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

    for i, row in df_vertices.iterrows():
        progress.value += 1
        elapsed = time.time() - throttler.window_start
        est_total = (elapsed / progress.value * len(df_vertices)) if progress.value else 0
        remaining = est_total - elapsed if est_total > elapsed else 0
        progress_label.value = f"Inserting vertex {progress.value} / {len(df_vertices)} - Est. time left: {remaining:.1f}s"

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

# --------------------------------------
# Upload Edges
# --------------------------------------
def upload_edges(client, df_edges, inserted_edge_keys, throttler):
    progress = IntProgress(min=0, max=len(df_edges), description='Edges:')
    progress_label = HTML()
    display(VBox([progress_label, progress]))

    for i, row in df_edges.iterrows():
        progress.value += 1
        elapsed = time.time() - throttler.window_start
        est_total = (elapsed / progress.value * len(df_edges)) if progress.value else 0
        remaining = est_total - elapsed if est_total > elapsed else 0
        progress_label.value = f"Inserting edge {progress.value} / {len(df_edges)} - Est. time left: {remaining:.1f}s"

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

# --------------------------------------
# Run Upload
# --------------------------------------
def run_graph_upload():
    throttler = RUThrottler(RU_LIMIT_PER_SECOND)

    print("Loading existing graph data...")
    inserted_vertex_ids, inserted_edge_keys = load_existing_graph(client, throttler)

    print("Uploading new vertices...")
    upload_vertices(client, df_vertices, inserted_vertex_ids, throttler)

    print("Uploading new edges...")
    upload_edges(client, df_edges, inserted_edge_keys, throttler)

    print(f"Upload complete. Estimated total RU consumed: {throttler.total_ru:.2f} RU across {throttler.total_ops} operations.")
    client.close()
