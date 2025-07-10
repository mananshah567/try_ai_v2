import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def load_existing_vertices(
    client: Client,
    throttler,
    batch_size: int = 1000,
    max_retries: int = 3,
    backoff_base: float = 1.0
):
    """
    Robustly load all existing vertices via continuation tokens, with retry/backoff,
    RU throttling, and a notebook progress bar (indeterminate if count fails).
    Returns a set of vertex IDs.
    """
    continuation = None
    inserted_vertex_ids = set()

    # 1) Try to get total count for sizing the bar
    try:
        total = client.submit(
            "g.V().count()", {}, request_options={"pageSize": 1}
        ).all().result()[0]
        bar = IntProgress(min=0, max=total, description="Vertices:")
    except GremlinServerError:
        total = None
        bar = IntProgress(min=0, description="Vertices:")  # indeterminate

    label = HTML()
    display(VBox([label, bar]))

    start = time.time()
    loaded = 0
    label.value = f"Loaded 0{'/' + str(total) if total else ''} vertices..."

    # Server-side page size, may adapt on timeout
    current_batch = batch_size

    query = "g.V().id()"

    while True:
        # 2) Fetch page with retry/backoff
        for attempt in range(max_retries):
            try:
                rs = client.submit(
                    query,
                    {},
                    request_options={
                        "continuation": continuation,
                        "pageSize": current_batch
                    }
                )
                verts = rs.all().result()
                ru = float(rs.status_attributes.get('x-ms-total-request-charge', 10.0))
                throttler.throttle(ru)
                break
            except GremlinServerError as e:
                msg = str(e)
                if 'GraphTimeoutException' in msg:
                    # halve batch and retry
                    current_batch = max(100, current_batch // 2)
                    time.sleep(backoff_base * (2 ** attempt))
                else:
                    raise
        else:
            print("Failed to fetch vertex batch after retries, aborting.")
            break

        if not verts:
            # no more vertices
            break

        # 3) Collect and update progress
        for vid in verts:
            inserted_vertex_ids.add(vid)
        loaded += len(verts)
        bar.value = loaded
        elapsed = time.time() - start
        rate = loaded / elapsed if elapsed else 0
        eta = (total - loaded) / rate if (total and rate) else float('inf')
        label.value = (
            f"Loaded {loaded}{'/' + str(total) if total else ''} vertices in {elapsed:.1f}s"
            f". ETA: {eta:.1f}s"
        )

        # 4) Advance continuation
        continuation = rs.status_attributes.get('x-ms-continuation')
        if not continuation:
            break

    return inserted_vertex_ids


def load_existing_edges(client: Client, throttler, batch_size: int = 1000):
    """
    Load all existing edges via continuation tokens, with a notebook progress bar.
    Returns a set of composite keys "out_label_in".
    """
    continuation = None
    inserted_edge_keys = set()

    # get total count
    total = client.submit("g.E().count()", {}, request_options={"pageSize":1}) \
                  .all().result()[0]
    edge_label = HTML()
    ebar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([edge_label, ebar]))

    start = time.time()
    edge_label.value = f"Loaded 0 / {total} edges…"

    query = """
      g.E()
       .project('out','label','in')
         .by(out().id())
         .by(label())
         .by(in().id())
    """
    loaded = 0

    while True:
        result_set = client.submit(
            query,
            {},
            request_options={"continuation": continuation, "pageSize": batch_size}
        )
        edges = result_set.all().result()
        attrs = result_set.status_attributes

        # throttle on RU
        ru = float(attrs.get("x-ms-total-request-charge", 10.0))
        throttler.throttle(ru)

        # collect & progress
        for e in edges:
            key = f"{e['out']}_{e['label']}_{e['in']}"
            inserted_edge_keys.add(key)
        loaded += len(edges)
        ebar.value = loaded
        elapsed = time.time() - start
        rate = loaded / elapsed if elapsed else 0
        est_left = (total - loaded) / rate if rate else float('inf')
        edge_label.value = (
            f"Loaded {loaded} / {total} edges in {elapsed:.1f}s. "
            f"Est time left: {est_left:.1f}s"
        )

        continuation = attrs.get("x-ms-continuation")
        if not continuation:
            break

    return inserted_edge_keys


# Example usage:
inserted_vertex_ids = load_existing_vertices(client, throttler, batch_size=500)
inserted_edge_keys   = load_existing_edges   (client, throttler, batch_size=500)
