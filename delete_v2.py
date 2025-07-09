import time
from itertools import islice
from gremlin_python.driver.client import Client
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


def delete_vertices_in_batches(
    client: Client,
    throttler,
    vertex_ids,
    batch_size: int = 100
) -> int:
    """
    Batch-delete known vertices by ID, with RU throttling and ETA display.

    - `vertex_ids`: iterable of vertex ID strings to delete.
    - `batch_size`: number of vertices to drop per Gremlin call.
    Returns the total number of vertices deleted.
    """
    vertex_ids = list(vertex_ids)
    total = len(vertex_ids)
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    deleted = 0
    processed = 0
    start = time.time()

    for batch in chunked(vertex_ids, batch_size):
        statements = [f"g.V('{vid}').drop()" for vid in batch]
        gremlin = "; ".join(statements)
        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        batch_count = len(batch)
        deleted += batch_count
        processed += batch_count

        bar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta = (total - processed) / rate if rate else float('inf')
        label.value = (
            f"Deleted {deleted}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        )

    return deleted


def delete_edges_in_batches(
    client: Client,
    throttler,
    edge_ids,
    batch_size: int = 100
) -> int:
    """
    Batch-delete known edges by ID, with RU throttling and ETA display.

    - `edge_ids`: iterable of edge ID strings to delete.
    - `batch_size`: number of edges to drop per Gremlin call.
    Returns the total number of edges deleted.
    """
    edge_ids = list(edge_ids)
    total = len(edge_ids)
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    deleted = 0
    processed = 0
    start = time.time()

    for batch in chunked(edge_ids, batch_size):
        statements = [f"g.E('{eid}').drop()" for eid in batch]
        gremlin = "; ".join(statements)
        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        batch_count = len(batch)
        deleted += batch_count
        processed += batch_count

        bar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta = (total - processed) / rate if rate else float('inf')
        label.value = (
            f"Deleted {deleted}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        )

    return deleted


def delete_all_edges_in_batches(
    client: Client,
    throttler,
    batch_size: int = 500
) -> int:
    """
    Batch-delete all edges in the graph without pre-known IDs, with RU throttling, progress bar, and ETA.

    - `batch_size`: how many edges to drop per loop.
    Returns the total number of edges deleted.
    """
    # Attempt to get total for progress bar
    try:
        total = client.submit("g.E().count()", {}).all().result()[0]
    except Exception:
        total = None

    label = HTML()
    if total is not None:
        bar = IntProgress(min=0, max=total, description="Edges:")
    else:
        bar = IntProgress(min=0, description="Edges:")  # indeterminate max
    display(VBox([label, bar]))

    deleted = 0
    start = time.time()

    while True:
        # fetch a batch of edge IDs
        rs = client.submit(f"g.E().limit({batch_size}).id()")
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)
        edge_ids = rs.all().result()

        if not edge_ids:
            break

        # drop them
        drop_script = "; ".join(f"g.E('{eid}').drop()" for eid in edge_ids)
        rs2 = client.submit(drop_script)
        attrs2 = rs2.status_attributes
        ru2 = float(attrs2.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru2)

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

    - `batch_size`: how many vertices to drop per loop.
    Returns the total number of vertices deleted.
    """
    # First delete edges to avoid dangling references
    total_vertices = None
    # Attempt to get total vertex count
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

    # delete all edges first
    delete_all_edges_in_batches(client, throttler, batch_size)

    while True:
        rs = client.submit(f"g.V().limit({batch_size}).id()")
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)
        vertex_ids = rs.all().result()

        if not vertex_ids:
            break

        drop_script = "; ".join(f"g.V('{vid}').drop()" for vid in vertex_ids)
        rs2 = client.submit(drop_script)
        attrs2 = rs2.status_attributes
        ru2 = float(attrs2.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru2)

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
