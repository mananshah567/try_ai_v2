import time
from itertools import islice
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def chunked(iterable, size):
    """Yield successive `size`-sized chunks from `iterable`."""
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
    Batch-delete all edges in the graph without pre-known IDs, with RU throttling, progress bar, and ETA.

    - `batch_size`: how many edges to process per loop.
    Returns the total number of edges deleted.
    """
    # Try to fetch total count for progress bar
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
        # get a batch of edge IDs
        rs = client.submit(f"g.E().limit({batch_size}).id()")
        attrs = rs.status_attributes
        throttler.throttle(float(attrs.get('x-ms-total-request-charge', 10.0)))
        edge_ids = rs.all().result()
        if not edge_ids:
            break

        # drop them with a single Gremlin traversal
        ids_literal = ",".join(f"'{eid}'" for eid in edge_ids)
        drop_query = f"g.E().hasId({ids_literal}).drop()"
        try:
            rs2 = client.submit(drop_query)
            rs2.all().result()
            attrs2 = rs2.status_attributes
            throttler.throttle(float(attrs2.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping edges batch {edge_ids}: {e}")
        else:
            deleted += len(edge_ids)
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
    Batch-delete all vertices in the graph without pre-known IDs, with RU throttling, progress bar, and ETA.

    - `batch_size`: how many vertices to process per loop.
    Returns the total number of vertices deleted.
    """
    # First clear edges
    total_vertices = None
    try:
        total_vertices = client.submit("g.V().count()", {}).all().result()[0]
    except Exception:
        total_vertices = None

    v_label = HTML()
    if total_vertices is not None:
        v_bar = IntProgress(min=0, max=total_vertices, description="Vertices:")
    else:
        v_bar = IntProgress(min=0, description="Vertices:")
    display(VBox([v_label, v_bar]))

    deleted = 0
    start = time.time()
    # remove edges first to avoid orphaned references
    delete_all_edges_in_batches(client, throttler, batch_size)

    while True:
        rs = client.submit(f"g.V().limit({batch_size}).id()")
        attrs = rs.status_attributes
        throttler.throttle(float(attrs.get('x-ms-total-request-charge', 10.0)))
        vertex_ids = rs.all().result()
        if not vertex_ids:
            break

        ids_literal = ",".join(f"'{vid}'" for vid in vertex_ids)
        drop_query = f"g.V().hasId({ids_literal}).drop()"
        try:
            rs2 = client.submit(drop_query)
            rs2.all().result()
            attrs2 = rs2.status_attributes
            throttler.throttle(float(attrs2.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping vertices batch {vertex_ids}: {e}")
        else:
            deleted += len(vertex_ids)
            v_bar.value = deleted
            elapsed = time.time() - start
            if total_vertices:
                rate = deleted / elapsed if elapsed else 0
                eta = (total_vertices - deleted) / rate if rate else float('inf')
                v_label.value = f"Deleted {deleted}/{total_vertices} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
            else:
                v_label.value = f"Deleted {deleted} vertices, Elapsed {elapsed:.1f}s"

    return deleted
