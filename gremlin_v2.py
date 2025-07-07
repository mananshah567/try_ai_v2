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
# Load Existing Vertices and Edges in Batches with Progress and ETA
# --------------------------------------
def load_existing_graph(client, throttler, batch_size=1000):
    inserted_vertex_ids = set()
    inserted_edge_keys = set()

    # Load vertices with progress bar + ETA
    print("Loading existing vertices...")
    vertex_offset = 0
    total_vertex_count = 0
    vertex_progress = IntProgress(min=0, max=1, description='Vertices:')
    vertex_label = HTML()
    display(VBox([vertex_label, vertex_progress]))
    vertex_start_time = time.time()

    while True:
        query = f"g.V().range({vertex_offset}, {vertex_offset + batch_size}).id()"
        try:
            result_set = client.submit(query)
            vertex_ids = result_set.all().result()
            if not vertex_ids:
                break
            inserted_vertex_ids.update(str(vid) for vid in vertex_ids)
            throttler.throttle(10)
            total_vertex_count += len(vertex_ids)
            vertex_progress.max = total_vertex_count + batch_size  # stretch as needed
            vertex_progress.value = total_vertex_count

            elapsed = time.time() - vertex_start_time
            est_total_batches = (vertex_progress.max // batch_size)
            batches_done = (total_vertex_count // batch_size)
            est_time_left = (elapsed / (batches_done + 1e-5)) * (est_total_batches - batches_done)
            vertex_label.value = f"Loaded {total_vertex_count} vertices so far... Estimated time left: {est_time_left:.1f}s"
            vertex_offset += batch_size
        except Exception as e:
            logger.error(f"Failed to load vertices: {e}")
            break

    print("Loading existing edges...")
    edge_offset = 0
    total_edge_count = 0
    edge_progress = IntProgress(min=0, max=1, description='Edges:')
    edge_label = HTML()
    display(VBox([edge_label, edge_progress]))
    edge_start_time = time.time()

    while True:
        query = f"g.E().range({edge_offset}, {edge_offset + batch_size}).project('out', 'label', 'in').by(outV().id()).by(label()).by(inV().id())"
        try:
            result_set = client.submit(query)
            edges = result_set.all().result()
            if not edges:
                break
            for e in edges:
                edge_key = f"{e['out']}-{e['label']}->{e['in']}"
                inserted_edge_keys.add(edge_key)
            throttler.throttle(10)
            total_edge_count += len(edges)
            edge_progress.max = total_edge_count + batch_size  # stretch as needed
            edge_progress.value = total_edge_count

            elapsed = time.time() - edge_start_time
            est_total_batches = (edge_progress.max // batch_size)
            batches_done = (total_edge_count // batch_size)
            est_time_left = (elapsed / (batches_done + 1e-5)) * (est_total_batches - batches_done)
            edge_label.value = f"Loaded {total_edge_count} edges so far... Estimated time left: {est_time_left:.1f}s"
            edge_offset += batch_size
        except Exception as e:
            logger.error(f"Failed to load edges: {e}")
            break

    print("Finished loading existing graph.")
    return inserted_vertex_ids, inserted_edge_keys
