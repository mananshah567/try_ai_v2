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
    Batch-delete vertices by ID, with RU throttling and ETA display.

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
        # build drop statements
        statements = [f"g.V('{vid}').drop()" for vid in batch]
        gremlin = "; ".join(statements)

        # submit and throttle
        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        # update counters
        batch_count = len(batch)
        deleted += batch_count
        processed += batch_count

        # update progress bar and ETA
        bar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta = (total - processed) / rate if rate else float('inf')
        label.value = (
            f"Deleted {deleted}/{total} vertices, "
            f"Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        )

    return deleted


def delete_edges_in_batches(
    client: Client,
    throttler,
    edge_ids,
    batch_size: int = 100
) -> int:
    """
    Batch-delete edges by ID, with RU throttling and ETA display.

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
        # build drop statements
        statements = [f"g.E('{eid}').drop()" for eid in batch]
        gremlin = "; ".join(statements)

        # submit and throttle
        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        # update counters
        batch_count = len(batch)
        deleted += batch_count
        processed += batch_count

        # update progress bar and ETA
        bar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta = (total - processed) / rate if rate else float('inf')
        label.value = (
            f"Deleted {deleted}/{total} edges, "
            f"Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        )

    return deleted
