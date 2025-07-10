import time
from itertools import islice
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

# Utility to chunk an iterable
def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def delete_all_edges_in_batches(
    client: Client,
    throttler,
    batch_size: int = 500
) -> int:
    """
    Sequentially delete all edges in the graph, batching by ID, with throttling and ETA.
    """
    # Attempt to get total count for progress bar
    try:
        total = client.submit("g.E().count()", {}).all().result()[0]
    except Exception:
        total = None

    label = HTML()
    if total is not None:
        bar = IntProgress(min=0, max=total, description="Edges:")
    else:
        bar = IntProgress(min=0, description="Edges:")
    display(VBox([label, bar]))

    deleted = 0
    start = time.time()

    while True:
        # fetch a page of edge IDs
        rs = client.submit(f"g.E().limit({batch_size}).id()", request_options={"pageSize": batch_size})
        ids = rs.all().result()
        throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))

        if not ids:
            break

        # drop them in one go
        query = f"g.E().hasId({','.join(f"'{i}'" for i in ids)}).drop()"
        try:
            rs2 = client.submit(query, request_options={"pageSize": len(ids)})
            rs2.all().result()
            throttler.throttle(float(rs2.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping edges {ids[:3]}...: {e}")
            break

        deleted += len(ids)
        bar.value = deleted
        elapsed = time.time() - start
        if total:
            rate = deleted / elapsed if elapsed else 0
            eta = (total - deleted) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        else:
            label.value = f"Deleted {deleted} edges, Elapsed {elapsed:.1f}s"

    return deleted


def delete_all_vertices_in_batches(
    client: Client,
    throttler,
    batch_size: int = 500
) -> int:
    """
    Sequentially delete all vertices in the graph, batching by ID, with throttling and ETA.
    """
    # First delete edges to avoid orphans
    deleted_edges = delete_all_edges_in_batches(client, throttler, batch_size)

    # Get total count for vertices
    try:
        total = client.submit("g.V().count()", {}).all().result()[0]
    except Exception:
        total = None

    label = HTML()
    if total is not None:
        bar = IntProgress(min=0, max=total, description="Vertices:")
    else:
        bar = IntProgress(min=0, description="Vertices:")
    display(VBox([label, bar]))

    deleted = 0
    start = time.time()

    while True:
        rs = client.submit(f"g.V().limit({batch_size}).id()", request_options={"pageSize": batch_size})
        ids = rs.all().result()
        throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))

        if not ids:
            break

        query = f"g.V().hasId({','.join(f"'{i}'" for i in ids)}).drop()"
        try:
            rs2 = client.submit(query, request_options={"pageSize": len(ids)})
            rs2.all().result()
            throttler.throttle(float(rs2.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping vertices {ids[:3]}...: {e}")
            break

        deleted += len(ids)
        bar.value = deleted
        elapsed = time.time() - start
        if total:
            rate = deleted / elapsed if elapsed else 0
            eta = (total - deleted) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        else:
            label.value = f"Deleted {deleted} vertices, Elapsed {elapsed:.1f}s"

    return deleted
