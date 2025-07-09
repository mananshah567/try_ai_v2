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


def delete_vertices_in_batches(
    client: Client,
    throttler,
    vertex_ids,
    batch_size: int = 100
) -> int:
    """
    Batch-delete known vertices by ID, with RU throttling and ETA display.
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
        drop_script = "; ".join(f"g.V('{vid}').drop()" for vid in batch)
        try:
            rs = client.submit(drop_script)
            rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping vertices {batch}: {e}")
        else:
            deleted += len(batch)
            processed += len(batch)
            bar.value = processed
            elapsed = time.time() - start
            rate = processed / elapsed if elapsed else 0
            eta = (total - processed) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
    return deleted


def delete_edges_in_batches(
    client: Client,
    throttler,
    edge_ids,
    batch_size: int = 100
) -> int:
    """
    Batch-delete known edges by ID, with RU throttling and ETA display.
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
        drop_script = "; ".join(f"g.E('{eid}').drop()" for eid in batch)
        try:
            rs = client.submit(drop_script)
            rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping edges {batch}: {e}")
        else:
            deleted += len(batch)
            processed += len(batch)
            bar.value = processed
            elapsed = time.time() - start
            rate = processed / elapsed if elapsed else 0
            eta = (total - processed) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
    return deleted


def delete_all_edges_in_batches(
    client: Client,
    throttler,
    batch_size: int = 500
) -> int:
    """
    Efficiently drop all edges in the graph in batches using a single-step drop with limit.
    """
    # try to get total count for progress
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
        # fetch and drop in one traversal
        query = (
            f"g.E().limit({batch_size})"
            ".as('e')"
            ".sideEffect(drop())"
            ".select('e').id()"
        )
        try:
            rs = client.submit(query, request_options={"pageSize": batch_size})
            dropped = rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping edge batch: {e}")
            break
        if not dropped:
            break
        deleted += len(dropped)
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
    Efficiently drop all vertices in the graph in batches using a single-step drop with limit.
    """
    # first clear edges
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
    delete_all_edges_in_batches(client, throttler, batch_size)
    while True:
        query = (
            f"g.V().limit({batch_size})"
            ".as('v')"
            ".sideEffect(drop())"
            ".select('v').id()"
        )
        try:
            rs = client.submit(query, request_options={"pageSize": batch_size})
            dropped = rs.all().result()
            throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
        except GremlinServerError as e:
            print(f"Failed dropping vertex batch: {e}")
            break
        if not dropped:
            break
        deleted += len(dropped)
        v_bar.value = deleted
        elapsed = time.time() - start
        if total_vertices:
            rate = deleted / elapsed if elapsed else 0
            eta = (total_vertices - deleted) / rate if rate else float('inf')
            v_label.value = f"Deleted {deleted}/{total_vertices} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        else:
            v_label.value = f"Deleted {deleted} vertices, Elapsed {elapsed:.1f}s"
    return deleted
