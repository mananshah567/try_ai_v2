import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import pandas as pd
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


def parallel_upload_vertices(
    client: Client,
    throttler,
    vertices,
    inserted_vertex_ids: set,
    dispatcher: dict,
    batch_size: int = 100,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Parallel upload of new vertices, issuing one Gremlin call per vertex to avoid multi-statement scripts.
    Supports DataFrame or list of dicts, with dispatcher for property functions.
    """
    # Normalize input
    if isinstance(vertices, pd.DataFrame):
        rows = list(vertices.itertuples(index=False, name='Row'))
        def vid(x): return str(getattr(x, 'ID')).strip()
        def lbl(x): return str(getattr(x, 'label')).strip()
    else:
        rows = list(vertices)
        def vid(x): return x['id']
        def lbl(x): return x['label']

    # Filter out already inserted
    tasks = [r for r in rows if vid(r) not in inserted_vertex_ids]
    total = len(tasks)

    # Progress UI
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_vertex(r):
        id_val = vid(r)
        lbl_val = lbl(r)
        base_q = f"g.addV('{lbl_val}').property('id','{id_val}')"
        prop_func = dispatcher.get(lbl_val)
        query = prop_func(r, base_q) if prop_func else base_q
        count = 0
        for attempt in range(max_retries):
            try:
                rs = client.submit(query)
                rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
                count = 1
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'TooManyRequests' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                    continue
                else:
                    print(f"Vertex upload failed for {id_val}: {e}")
                    break
        return count, id_val

    # Dispatch in parallel threads
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_upload_vertex, r): r for r in tasks}
        for future in as_completed(futures):
            count, vid_val = future.result()
            with lock:
                if count:
                    inserted_vertex_ids.add(vid_val)
                uploaded += count
                bar.value = uploaded
                elapsed = time.time() - start
                rate = uploaded / elapsed if elapsed else 0
                eta = (total - uploaded) / rate if rate else float('inf')
                label.value = f"Uploaded {uploaded}/{total} vertices, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"

    return inserted_vertex_ids


def parallel_upload_edges(
    client: Client,
    throttler,
    edges,
    inserted_edge_keys: set,
    batch_size: int = 100,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Parallel upload of new edges, issuing one Gremlin call per edge to avoid multi-statement scripts.
    Supports DataFrame or list of dicts.
    """
    # Normalize input
    if isinstance(edges, pd.DataFrame):
        df = edges
        rows = list(df.itertuples(index=False, name='Edge'))
        def out_id(r): return str(getattr(r, 'source')).strip()
        def in_id(r): return str(getattr(r, 'target')).strip()
        def lbl(r):
            val = getattr(r, 'edge_detail') if hasattr(r, 'edge_detail') else None
            return val if pd.notna(val) else 'connects'
        prop_cols = [c for c in df.columns if c not in ('source','target','edge_detail')]
        def props(r): return {c: getattr(r, c) for c in prop_cols if pd.notna(getattr(r, c))}
    else:
        rows = list(edges)
        def out_id(r): return r['out']
        def in_id(r): return r['in']
        def lbl(r): return r['label']
        def props(r): return r.get('props', {})

    # Filter new edges
    tasks = []
    for r in rows:
        key = f"{out_id(r)}_{lbl(r)}_{in_id(r)}"
        if key not in inserted_edge_keys:
            tasks.append((r, key))
    total = len(tasks)

    # Progress UI
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_edge(args):
        r, key = args
        outv, inv, lblv = out_id(r), in_id(r), lbl(r)
        query = f"g.V('{outv}').addE('{lblv}').to(g.V('{inv}')).property('id','{key}')"
        for k, v in props(r).items():
            query += f".property('{k}','{v}')"
        count = 0
        for attempt in range(max_retries):
            try:
                rs = client.submit(query)
                rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge', 10.0)))
                count = 1
                break
            except GremlinServerError as e:
                msg = str(e)
                if '429' in msg or 'GraphTimeoutException' in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                    continue
                else:
                    print(f"Edge upload failed for {key}: {e}")
                    break
        return count, key

    # Dispatch in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_upload_edge, task): task for task in tasks}
        for future in as_completed(futures):
            count, key = future.result()
            with lock:
                if count:
                    inserted_edge_keys.add(key)
                uploaded += count
                bar.value = uploaded
                elapsed = time.time() - start
                rate = uploaded / elapsed if elapsed else 0
                eta = (total - uploaded) / rate if rate else float('inf')
                label.value = f"Uploaded {uploaded}/{total} edges, Elapsed {elapsed:.1f}s, ETA {eta:.1f}s"

    return inserted_edge_keys
