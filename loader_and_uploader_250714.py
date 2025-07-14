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
    # Normalize to rows and safely extract fields, guarding against None/NaN/empty
if isinstance(vertices, pd.DataFrame):
    rows = list(vertices.itertuples(index=False, name='Row'))
    def _safe(val):
        # handle None, NaN
        import pandas as _pd
        if val is None or (_pd.isna(val) if isinstance(val, float) or _pd.isna(val) else False):
            return ''
        s = str(val).strip()
        return s if s else ''
    def vid(r):   return _safe(getattr(r, 'ID', None))
    def lbl(r):   return _safe(getattr(r, 'Label', None))
    def etype(r): return _safe(getattr(r, 'entity_type', None))
    def ecate(r): return _safe(getattr(r, 'entity_category', None))
else:
    rows = list(vertices)
    def _safe(val):
        import pandas as _pd
        if val is None or (_pd.isna(val) if isinstance(val, float) or _pd.isna(val) else False):
            return ''
        s = str(val).strip()
        return s if s else ''
    def vid(r):   return _safe(r.get('id', None))
    def lbl(r):   return _safe(r.get('label', None))
    def etype(r): return _safe(r.get('entity_type', None))
    def ecate(r): return _safe(r.get('entity_category', None))
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
