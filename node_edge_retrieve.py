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
            ru = result_set.status_attributes.get('x-ms-total-request-charge', 10.0)
            if not vertex_ids:
                break
            inserted_vertex_ids.update(str(vid) for vid in vertex_ids)
            throttler.throttle(ru)
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
            ru = result_set.status_attributes.get('x-ms-total-request-charge', 10.0)
            if not edges:
                break
            for e in edges:
                edge_key = f"{e['out']}-{e['label']}->{e['in']}"
                inserted_edge_keys.add(edge_key)
            throttler.throttle(ru)
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
