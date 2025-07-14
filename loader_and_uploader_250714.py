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

# Utility: safe string extraction
def _safe(val):
    import pandas as _pd
    if val is None:
        return ''
    try:
        if isinstance(val, float) and _pd.isna(val):
            return ''
    except:
        pass
    s = str(val).strip()
    return s if s else ''


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
    try:
        total = client.submit(
            "g.V().count()", {}, request_options={"pageSize":1}
        ).all().result()[0]
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
        # Build sorted, paged query projecting needed props
        proj = (
            ".project('id','label','entity_type','entity_category')"
            ".by(id())"
            ".by(label())"
            ".by(values('entity_type').fold().coalesce(unfold(),constant('')))"
            ".by(values('entity_category').fold().coalesce(unfold(),constant('')))"
        )
        if last_gid is None:
            gremlin = (
                f"g.V().order().by('graph_id').limit({batch_size})" + proj + ".values('graph_id')"
            )
        else:
            gremlin = (
                f"g.V().has('graph_id', P.gt('{last_gid}')).order().by('graph_id')"
                f".limit({batch_size})" + proj + ".values('graph_id')"
            )
        # Retry fetch
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

        # Combine and record
        for gid in batch:
            # fetch the projected values for each id
            # here 'batch' items are the graph_id values; fetch props separately
            pass  # handle below
        # Actually fetch full projection for this batch
        # Build detail query
        detail_gremlin = (
            f"g.V().has('graph_id', within({','.join(f"'{gid}'" for gid in batch)}))"
            ".project('id','label','entity_type','entity_category')"
            ".by(id())"
            ".by(label())"
            ".by(values('entity_type').fold().coalesce(unfold(),constant('')))"
            ".by(values('entity_category').fold().coalesce(unfold(),constant('')))"
        )
        rs2 = client.submit(detail_gremlin, request_options={"pageSize": len(batch)})
        items = rs2.all().result()
        throttler.throttle(float(rs2.status_attributes.get('x-ms-total-request-charge',10.0)))

        for item in items:
            key = '|'.join(_safe(item[k]) for k in ('id','label','entity_type','entity_category'))
            inserted.add(key)
        loaded += len(items)
        last_gid = batch[-1]
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
    inserted_keys: set,
    dispatcher: dict,
    batch_size: int = 100,
    workers: int = 4,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Parallel upload of new vertices, comparing composite keys:
    "id|Label|entity_type|entity_category".
    """
    # Normalize rows and extract
    if isinstance(vertices, pd.DataFrame):
        rows = list(vertices.itertuples(index=False, name='Row'))
    else:
        rows = list(vertices)

    def extract(r):
        if hasattr(r, '_fields'):
            idv = _safe(getattr(r, 'ID', None))
            lb  = _safe(getattr(r, 'Label', None))
            et  = _safe(getattr(r, 'entity_type', None))
            ec  = _safe(getattr(r, 'entity_category', None))
        else:
            idv = _safe(r.get('id'))
            lb  = _safe(r.get('label'))
            et  = _safe(r.get('entity_type'))
            ec  = _safe(r.get('entity_category'))
        return idv, lb, et, ec

    tasks = []
    for r in rows:
        idv, lb, et, ec = extract(r)
        key = f"{idv}|{lb}|{et}|{ec}"
        if key not in inserted_keys:
            tasks.append((r, key))
    total = len(tasks)

    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    uploaded = 0
    lock = threading.Lock()
    start = time.time()

    def _upload_task(arg):
        r, key = arg
        idv, lb, et, ec = extract(r)
        # base Gremlin
        q = (
            f"g.addV('{lb}')"
            f".property('id','{idv}')"
            f".property('graph_id','{idv}')"
        )
        q = dispatcher.get(lb, lambda row, qq: qq)(r, q)
        # retry
        for attempt in range(max_retries):
            try:
                rs = client.submit(q)
                rs.all().result()
                throttler.throttle(float(rs.status_attributes.get('x-ms-total-request-charge',10.0)))
                return True
            except GremlinServerError as e:
                if '429' in str(e) or 'GraphTimeoutException' in str(e):
                    time.sleep(backoff_base * (2**attempt))
                    continue
                else:
                    print(f"Upload failed for {key}: {e}")
                    return False
        return False

    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(_upload_task, t): t for t in tasks}
        for future in as_completed(futures):
            success = future.result()
            with lock:
                if success:
                    uploaded += 1
                    inserted_keys.add(futures[future][1])
                bar.value = uploaded
                elapsed = time.time() - start
                rate = uploaded/elapsed if elapsed else 0
                eta = (total-uploaded)/rate if rate else float('inf')
                label.value = f"Uploaded {uploaded}/{total} vertices · ETA {eta:.1f}s"

    return inserted_keys
