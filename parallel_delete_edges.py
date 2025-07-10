import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def parallel_delete_edges(
    client: Client,
    throttler,
    all_edge_ids,
    batch_size: int = 2000,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> int:
    """
    Delete all edges in parallel, batching and retrying on rate limits.

    Parameters:
    - client: Gremlin Client
    - throttler: RU throttler
    - all_edge_ids: iterable of edge ID strings to delete
    - batch_size: number of edges per batch
    - workers: parallel threads
    - max_retries: retry attempts for 429/timeouts
    - backoff_base: base seconds for exponential backoff

    Returns total number of edges deleted.
    """
    edge_ids = list(all_edge_ids)
    total = len(edge_ids)

    # Progress UI
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    deleted = 0
    lock = threading.Lock()
    start = time.time()

    def _drop_batch(batch_ids):
        nonlocal deleted
        local_count = 0
        # attempt drop with retries
        for attempt in range(max_retries):
            ids_literal = ",".join(f"'{eid}'" for eid in batch_ids)
            query = f"g.E().hasId({ids_literal}).drop()"
            try:
                rs = client.submit(query, request_options={"pageSize": len(batch_ids)})
                rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
                local_count = len(batch_ids)
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'TooManyRequests' in msg:
                    wait = backoff_base * (2 ** attempt)
                    print(f"Rate-limited, retry in {wait:.1f}s (attempt {attempt+1})")
                    time.sleep(wait)
                    continue
                else:
                    print(f"Failed dropping edges {batch_ids[:3]}...: {e}")
                    break

        # update shared progress
        with lock:
            deleted += local_count
            bar.value = deleted
            elapsed = time.time() - start
            rate = deleted / elapsed if elapsed else 0
            eta = (total - deleted) / rate if rate else float('inf')
            label.value = f"Deleted {deleted}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"
        return local_count

    # dispatch in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_drop_batch, batch) for batch in chunked(edge_ids, batch_size)]
        for _ in as_completed(futures):
            pass

    return deleted

# Usage Example:
# deleted = parallel_delete_edges(
#     client,
#     throttler,
#     all_edge_ids,
#     batch_size=2000,
#     workers=4,
#     max_retries=3,
#     backoff_base=1.0
# )
