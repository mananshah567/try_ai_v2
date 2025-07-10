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
    batch_size: int = 500,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> int:
    """
    Batch-delete all edges in the graph without pre-known IDs, with retry/backoff, RU throttling, progress bar, and ETA.

    - `batch_size`: how many edges to process per loop.
    - `max_retries`: retry attempts for rate-limit or timeout errors.
    - `backoff_base`: base seconds for exponential backoff.
    Returns the total number of edges deleted.
    """
    # Attempt to fetch total count for progress bar
    try:
        total = client.submit("g.E().count()", {}).all().result()[0]
    except Exception:
        total = None

    label = HTML()
    bar = IntProgress(min=0, max=total if total is not None else 0, description="Edges:")
    display(VBox([label, bar]))

    deleted = 0
    start = time.time()

    while True:
        # fetch a batch of edge IDs with retry
        for attempt in range(max_retries):
            try:
                rs = client.submit(
                    f"g.E().limit({batch_size}).id()",
                    request_options={"pageSize": batch_size}
                )
                ids = rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                else:
                    raise
        else:
            # exhausted retries
            print("Failed to fetch edge IDs after retries, aborting.")
            break

        if not ids:
            break

        # drop the fetched IDs with retry
        ids_literal = ",".join(f"'{eid}'" for eid in ids)
        drop_query = f"g.E().hasId({ids_literal}).drop()"
        for attempt in range(max_retries):
            try:
                rs2 = client.submit(
                    drop_query,
                    request_options={"pageSize": len(ids)}
                )
                rs2.all().result()
                throttler.throttle(float(rs2.status_attributes.get('x-ms-total-request-charge', 10.0)))
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                else:
                    raise

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
    batch_size: int = 500,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> int:
    """
    Batch-delete all vertices in the graph without pre-known IDs, with retry/backoff, RU throttling, progress bar, and ETA.

    - `batch_size`: how many vertices to process per loop.
    - `max_retries`: retry attempts for rate-limit or timeout errors.
    - `backoff_base`: base seconds for exponential backoff.
    Returns the total number of vertices deleted.
    """
    # First delete edges to avoid dangling references
    deleted_edges = delete_all_edges_in_batches(
        client, throttler, batch_size, max_retries, backoff_base
    )

    # Attempt to fetch total vertex count
    try:
        total = client.submit("g.V().count()", {}).all().result()[0]
    except Exception:
        total = None

    label = HTML()
    bar = IntProgress(min=0, max=total if total is not None else 0, description="Vertices:")
    display(VBox([label, bar]))

    deleted = 0
    start = time.time()

    while True:
        # fetch a batch of vertex IDs with retry
        for attempt in range(max_retries):
            try:
                rs = client.submit(
                    f"g.V().limit({batch_size}).id()",
                    request_options={"pageSize": batch_size}
                )
                ids = rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                else:
                    raise
        else:
            print("Failed to fetch vertex IDs after retries, aborting.")
            break

        if not ids:
            break

        # drop fetched vertices with retry
        ids_literal = ",".join(f"'{vid}'" for vid in ids)
        drop_query = f"g.V().hasId({ids_literal}).drop()"
        for attempt in range(max_retries):
            try:
                rs2 = client.submit(
                    drop_query,
                    request_options={"pageSize": len(ids)}
                )
                rs2.all().result()
                throttler.throttle(float(rs2.status_attributes.get('x-ms-total-request-charge', 10.0)))
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                else:
                    raise

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
