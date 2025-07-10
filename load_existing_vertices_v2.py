import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def load_existing_vertices(
    client: Client,
    throttler,
    batch_size: int = 1000,
    minimal_batch: int = 100,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Robustly load all existing vertices via continuation tokens, with dynamic batch resizing on timeouts,
    retry/backoff on rate limits, RU throttling, and a notebook progress bar.
    Returns a set of vertex IDs.
    """
    continuation = None
    inserted_vertex_ids = set()

    # Get total count for progress bar (if possible)
    try:
        total = client.submit(
            "g.V().count()", {}, request_options={"pageSize":1}
        ).all().result()[0]
        vbar = IntProgress(min=0, max=total, description="Vertices:")
    except Exception:
        total = None
        vbar = IntProgress(min=0, description="Vertices:")

    vlabel = HTML()
    display(VBox([vlabel, vbar]))

    start = time.time()
    loaded = 0
    vlabel.value = f"Loaded 0{'/' + str(total) if total else ''} vertices..."

    current_batch = batch_size

    while True:
        # Dynamic fetch with batch-size reduction on timeout
        fetch_attempts = 0
        while True:
            try:
                rs = client.submit(
                    "g.V().id()",
                    {},
                    request_options={"continuation": continuation, "pageSize": current_batch}
                )
                verts = rs.all().result()
                ru = float(rs.status_attributes.get('x-ms-total-request-charge', 10.0))
                throttler.throttle(ru)
                break
            except GremlinServerError as e:
                msg = str(e)
                if 'GraphTimeoutException' in msg or '429' in msg:
                    fetch_attempts += 1
                    if fetch_attempts >= max_retries:
                        if current_batch <= minimal_batch:
                            print("⚠️ Fetch aborted: batch_size minimal; unable to proceed.")
                            return inserted_vertex_ids
                        # reduce batch and reset
                        current_batch = max(minimal_batch, current_batch // 2)
                        print(f"⚡ Reducing batch_size to {current_batch} and retrying...")
                        fetch_attempts = 0
                        time.sleep(backoff_base)
                    else:
                        time.sleep(backoff_base * (2 ** (fetch_attempts - 1)))
                    continue
                else:
                    raise

        if not verts:
            break

        # Collect & update
        for vid in verts:
            inserted_vertex_ids.add(vid)
        loaded += len(verts)
        vbar.value = loaded
        elapsed = time.time() - start
        rate = loaded / elapsed if elapsed else 0
        eta = (total - loaded) / rate if (total and rate) else float('inf')
        vlabel.value = (
            f"Loaded {loaded}{'/' + str(total) if total else ''} vertices. "
            f"Elapsed {elapsed:.1f}s. ETA {eta:.1f}s"
        )

        continuation = rs.status_attributes.get('x-ms-continuation')
        if not continuation:
            break

    return inserted_vertex_ids
