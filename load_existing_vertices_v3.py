import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.traversal import P
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def load_existing_vertices_by_keyset(
    client: Client,
    throttler,
    batch_size: int = 1000,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> set:
    """
    Load all vertex IDs by keyset pagination on 'graph_id', which you always set = id.
    This ensures each batch is the next slice in sorted order.
    """
    # 1) Try to size the progress bar
    try:
        total = client.submit(
            "g.V().count()", {}, request_options={"pageSize": 1}
        ).all().result()[0]
        bar = IntProgress(min=0, max=total, description="Vertices:")
    except Exception:
        total = None
        bar = IntProgress(min=0, description="Vertices:")
    label = HTML(); display(VBox([label, bar]))

    inserted = set()
    last_graph_id = None
    loaded = 0
    start = time.time()

    while True:
        # 2) Build ordered query
        if last_graph_id is None:
            gremlin = (
                f"g.V()"
                f".order().by('graph_id')"        # ensure sorted
                f".limit({batch_size})"
                f".values('graph_id')"            # return your id-property
            )
        else:
            gremlin = (
                f"g.V()"
                f".has('graph_id', P.gt('{last_graph_id}'))"
                f".order().by('graph_id')"
                f".limit({batch_size})"
                f".values('graph_id')"
            )

        # 3) Retry fetch on 429/timeout
        for attempt in range(max_retries):
            try:
                rs = client.submit(
                    gremlin,
                    request_options={"pageSize": batch_size}
                )
                ids = rs.all().result()
                ru = float(rs.status_attributes.get("x-ms-total-request-charge", 10.0))
                throttler.throttle(ru)
                break
            except GremlinServerError as e:
                msg = str(e)
                if "429" in msg or "GraphTimeoutException" in msg:
                    time.sleep(backoff_base * (2 ** attempt))
                    continue
                else:
                    raise
        else:
            print("❌ Failed to fetch batch after retries; aborting.")
            break

        if not ids:
            # we've paged through everything
            break

        # 4) Record & display progress
        inserted.update(ids)
        loaded += len(ids)
        last_graph_id = ids[-1]
        bar.value = loaded

        elapsed = time.time() - start
        rate = loaded / elapsed if elapsed else 0
        eta = (total - loaded) / rate if (total and rate) else float("inf")
        label.value = (
            f"Loaded {loaded}{'/' + str(total) if total else ''} "
            f"in {elapsed:.1f}s · ETA {eta:.1f}s"
        )

    return inserted
