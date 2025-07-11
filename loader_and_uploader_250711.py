import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.traversal import P
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import pandas as pd

# Utility: chunk an iterable into lists of size `size`
def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def load_existing_vertices_by_keyset(
    client: Client,
    throttler,
    batch_size: int = 1000,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Load all existing vertices by keyset pagination on 'graph_id', returning composite keys:
    "id|Label|entity_type|entity_category".
    """
    # Progress bar setup
    try:
        total = client.submit("g.V().count()", {}, request_options={"pageSize":1}).all().result()[0]
        bar = IntProgress(min=0, max=total, description="Vertices:")
    except:
        total = None
        bar = IntProgress(min=0, description="Vertices:")
    label = HTML(); display(VBox([label, bar]))

    inserted = set()
    last_gid = None
    loaded = 0
    start = time.time()

    while True:
        # build sorted, paged query projecting needed props
        if last_gid is None:
            gremlin = (
                f"g.V()"
                f".order().by('graph_id')"
                f".limit({batch_size})"
                f".project('id','label','entity_type','entity_category')"
                f".by(id())"
                f".by(label())"
                f".by(values('entity_type').fold().coalesce(unfold(),constant('')))"
                f".by(values('entity_category').fold().coalesce(unfold(),constant('')))"
            )
        else:
            gremlin = (
                f"g.V()"
                f".has('graph_id', P.gt('{last_gid}'))"
                f".order().by('graph_id')"
                f".limit({batch_size})"
                f".project('id','label','entity_type','entity_category')"
                f".by(id())"
                f".by(label())"
                f".by(values('entity_type').fold().coalesce(unfold(),constant('')))"
                f".by(values('entity_category').fold().coalesce(unfold(),constant('')))"
            )
        # fetch with retry/backoff
        for attempt in range(max_retries):
            try:
                rs = client.submit(gremlin, request_options={"pageSize": batch_size})
                batch = rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge',10.0)))
                break
            except GremlinServerError as e:
                if '429' in str(e) or 'GraphTimeoutException' in str(e):
                    time.sleep(backoff_base * (2**attempt))
                    continue
                else:
                    raise
        else:
            print("❌ Exhausted retries; aborting.")
            break

        if not batch:
            break

        # combine and record
        for item in batch:
            key = f"{item['id']}|{item['label']}|{item['entity_type']}|{item['entity_category']}"
            inserted.add(key)
        loaded += len(batch)
        last_gid = batch[-1]['id']
        bar.value = loaded
        elapsed = time.time() - start
        rate = loaded/elapsed if elapsed else 0
        eta = (total-loaded)/rate if total and rate else float('inf')
        label.value = f"Loaded {loaded}{'/' + str(total) if total else ''} vertices · ETA {eta:.1f}s"

    return inserted


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
    Parallel upload of new vertices from a DataFrame or list, comparing composite keys:
    "id|Label|entity_type|entity_category".
    """
    # Normalize to rows
    if isinstance(vertices, pd.DataFrame):
        rows = list(vertices.itertuples(index=False, name='Row'))
        def vid(r): return str(getattr(r, 'ID')).strip()
        def lbl(r): return str(getattr(r, 'Label')).strip()
        def etype(r): return str(getattr(r, 'entity_type')).strip()
        def ecate(r): return str(getattr(r, 'entity_category')).strip()
    else:
        rows = list(vertices)
        def vid(r): return r['id']
        def lbl(r): return r['label']
        def etype(r): return r['entity_type']
        def ecate(r): return r['entity_category']

    # Filter out existing
    tasks = []
    for r in rows:
        key = f"{vid(r)}|{lbl(r)}|{etype(r)}|{ecate(r)}"
        if key not in inserted_vertex_ids:
            tasks.append((r, key))
    total = len(tasks)

    # Progress UI
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_batch(batch):
        nonlocal uploaded
        count = 0
        for r, key in batch:
            id_val, label_val = vid(r), lbl(r)
            gid = id_val
            base_q = (
                f"g.addV('{label_val}')"
                f".property('id','{id_val}')"
                f".property('graph_id','{gid}')"
            )
            prop_q = dispatcher.get(label_val, lambda row, q: q)(r, base_q)
            for attempt in range(max_retries):
                try:
                    rs = client.submit(prop_q)
                    rs.all().result()
                    throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge',10.0)))
                    count += 1
                    inserted_vertex_ids.add(key)
                    break
                except GremlinServerError as e:
                    if '429' in str(e) or 'GraphTimeoutException' in str(e):
                        time.sleep(backoff_base * (2**attempt))
                        continue
                    else:
                        print(f"Failed {key}: {e}")
                        break
        with lock:
            uploaded += count
            bar.value = uploaded
            elapsed = time.time() - start
            rate = uploaded/elapsed if elapsed else 0
            eta = (total-uploaded)/rate if rate else float('inf')
            label.value = f"Uploaded {uploaded}/{total} vertices · ETA {eta:.1f}s"

    # Dispatch parallel
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = [exe.submit(_upload_batch, batch)
                   for batch in chunked(tasks, batch_size)]
        for _ in as_completed(futures):
            pass

    return inserted_vertex_ids
