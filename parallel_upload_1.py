import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def parallel_upload_vertices(
    client: Client,
    throttler,
    vertices,
    inserted_vertex_ids,
    batch_size: int = 100,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
):
    """
    Parallel batch-upload of new vertices with retry/backoff on rate limits or timeouts.
    - `vertices`: iterable of dicts {'id','label', optional 'props'}
    - `inserted_vertex_ids`: set to track already-uploaded IDs
    Returns updated `inserted_vertex_ids`.
    """
    to_upload = [v for v in vertices if v['id'] not in inserted_vertex_ids]
    total = len(to_upload)

    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_batch(batch):
        nonlocal uploaded
        count = 0
        # build Gremlin multi-statement script
        statements = []
        for v in batch:
            stmt = f"g.addV('{v['label']}').property('id','{v['id']}')"
            for k, val in v.get('props', {}).items():
                stmt += f".property('{k}','{val}')"
            statements.append(stmt)
        script = "; ".join(statements)

        for attempt in range(max_retries):
            try:
                rs = client.submit(script)
                rs.all().result()
                ru = float(rs.status_attributes.get('x-ms-total-request-charge', 10.0))
                throttler.throttle(ru)
                count = len(batch)
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'TooManyRequests' in msg or 'GraphTimeoutException' in msg:
                    wait = backoff_base * (2 ** attempt)
                    time.sleep(wait)
                    continue
                else:
                    print(f"Upload failed for vertices { [v['id'] for v in batch] }: {e}")
                    break

        with lock:
            for v in batch[:count]:
                inserted_vertex_ids.add(v['id'])
            uploaded += count
            bar.value = uploaded
            elapsed = time.time() - start
            rate = uploaded / elapsed if elapsed else 0
            eta = (total - uploaded) / rate if rate else float('inf')
            label.value = f"Uploaded {uploaded}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"

        return count

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_upload_batch, batch)
                   for batch in chunked(to_upload, batch_size)]
        for _ in as_completed(futures):
            pass

    return inserted_vertex_ids


def parallel_upload_edges(
    client: Client,
    throttler,
    edges,
    inserted_edge_keys,
    batch_size: int = 100,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
):
    """
    Parallel batch-upload of new edges with retry/backoff on rate limits or timeouts.
    - `edges`: iterable of dicts {'out','in','label', optional 'props'}
    - `inserted_edge_keys`: set to track existing composite keys
    Returns updated `inserted_edge_keys`.
    """
    to_upload = [e for e in edges if f"{e['out']}_{e['label']}_{e['in']}" not in inserted_edge_keys]
    total = len(to_upload)

    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_batch(batch):
        nonlocal uploaded
        count = 0
        statements = []
        keys = []
        for e in batch:
            key = f"{e['out']}_{e['label']}_{e['in']}"
            keys.append(key)
            stmt = (
                f"g.V('{e['out']}').addE('{e['label']}').to(g.V('{e['in']}')).property('id','{key}')"
            )
            for k, val in e.get('props', {}).items():
                stmt += f".property('{k}','{val}')"
            statements.append(stmt)
        script = "; ".join(statements)

        for attempt in range(max_retries):
            try:
                rs = client.submit(script)
                rs.all().result()
                ru = float(rs.status_attributes.get('x-ms-total-request-charge', 10.0))
                throttler.throttle(ru)
                count = len(batch)
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'TooManyRequests' in msg or 'GraphTimeoutException' in msg:
                    wait = backoff_base * (2 ** attempt)
                    time.sleep(wait)
                    continue
                else:
                    print(f"Upload failed for edges { keys[:3] }...: {e}")
                    break

        with lock:
            for key in keys[:count]:
                inserted_edge_keys.add(key)
            uploaded += count
            bar.value = uploaded
            elapsed = time.time() - start
            rate = uploaded / elapsed if elapsed else 0
            eta = (total - uploaded) / rate if rate else float('inf')
            label.value = f"Uploaded {uploaded}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"

        return count

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_upload_batch, batch)
                   for batch in chunked(to_upload, batch_size)]
        for _ in as_completed(futures):
            pass

    return inserted_edge_keys

# Example usage:
# new_vertices = [...]
# inserted_vertex_ids = parallel_upload_vertices(client, throttler, new_vertices, inserted_vertex_ids)
# new_edges = [...]
# inserted_edge_keys = parallel_upload_edges(client, throttler, new_edges, inserted_edge_keys)
