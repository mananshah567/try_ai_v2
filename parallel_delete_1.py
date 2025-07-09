import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

# utility to chunk an iterable
from itertools import islice
def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def parallel_delete_edges(
    client: Client,
    throttler,
    all_edge_ids,
    batch_size: int = 2000,
    workers: int = 4
) -> int:
    """
    Delete all edges in parallel batches with large pageSize.
    - all_edge_ids: full list of edge IDs to drop
    - batch_size: number of IDs per batch
    - workers: number of concurrent worker threads
    Returns total edges deleted.
    """
    all_edge_ids = list(all_edge_ids)
    total = len(all_edge_ids)

    # progress UI
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    deleted = 0
    deleted_lock = threading.Lock()
    start = time.time()

    def _drop_batch(batch_ids):
        nonlocal deleted
        # single Gremlin query: drop by hasId
        ids_literal = ",".join(f"'{eid}'" for eid in batch_ids)
        query = f"g.E().hasId({ids_literal}).drop()"
        try:
            rs = client.submit(
                query,
                request_options={"pageSize": len(batch_ids)}
            )
            rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
            count = len(batch_ids)
        except GremlinServerError as e:
            print(f"Failed dropping edges {batch_ids}: {e}")
            count = 0

        # update shared progress
        with deleted_lock:
            nonlocal_deleted = deleted + count
            deleted = nonlocal_deleted
            bar.value = deleted
            elapsed = time.time() - start
            rate = deleted / elapsed if elapsed else 0
            eta = (total - deleted) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        return count

    # dispatch in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_drop_batch, batch)
                   for batch in chunked(all_edge_ids, batch_size)]
        for f in as_completed(futures):
            pass

    return deleted


def parallel_delete_vertices(
    client: Client,
    throttler,
    all_vertex_ids,
    batch_size: int = 2000,
    workers: int = 4
) -> int:
    """
    Delete all vertices in parallel batches with large pageSize.
    - all_vertex_ids: full list of vertex IDs to drop
    - batch_size: number of IDs per batch
    - workers: number of concurrent worker threads
    Returns total vertices deleted.
    """
    all_vertex_ids = list(all_vertex_ids)
    total = len(all_vertex_ids)

    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    deleted = 0
    deleted_lock = threading.Lock()
    start = time.time()

    def _drop_batch(batch_ids):
        nonlocal deleted
        ids_literal = ",".join(f"'{vid}'" for vid in batch_ids)
        query = f"g.V().hasId({ids_literal}).drop()"
        try:
            rs = client.submit(
                query,
                request_options={"pageSize": len(batch_ids)}
            )
            rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
            count = len(batch_ids)
        except GremlinServerError as e:
            print(f"Failed dropping vertices {batch_ids}: {e}")
            count = 0

        with deleted_lock:
            nonlocal_deleted = deleted + count
            deleted = nonlocal_deleted
            bar.value = deleted
            elapsed = time.time() - start
            rate = deleted / elapsed if elapsed else 0
            eta = (total - deleted) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        return count

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_drop_batch, batch)
                   for batch in chunked(all_vertex_ids, batch_size)]
        for f in as_completed(futures):
            pass

    return deleted

# Example usage:
# all_edges = load_existing_edges(client, throttler)
# deleted_edges = parallel_delete_edges(client, throttler, all_edges, batch_size=2000, workers=4)
# 
# all_vertices = load_existing_vertices(client, throttler)
# deleted_vertices = parallel_delete_vertices(client, throttler, all_vertices, batch_size=2000, workers=4)
