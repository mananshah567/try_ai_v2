import time
from gremlin_python.driver.client import Client
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def load_existing_vertices(client: Client, throttler, batch_size: int = 1000):
    """
    Load all existing vertices via continuation tokens, with a notebook progress bar.
    Returns a set of vertex‐IDs.
    """
    continuation = None
    inserted_vertex_ids = set()

    # first get total count for sizing the bar
    total = client.submit("g.V().count()", {}, request_options={"pageSize":1}) \
                  .all().result()[0]
    vertex_label = HTML() 
    vbar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([vertex_label, vbar]))

    start = time.time()
    vertex_label.value = f"Loaded 0 / {total} vertices…"

    query = "g.V().id()"
    loaded = 0

    while True:
        result_set = client.submit(
            query,
            {},
            request_options={"continuation": continuation, "pageSize": batch_size}
        )
        verts = result_set.all().result()
        attrs = result_set.status_attributes

        # throttle on RU
        ru = float(attrs.get("x-ms-total-request-charge", 10.0))
        throttler.throttle(ru)

        # collect & update progress
        for vid in verts:
            inserted_vertex_ids.add(vid)
        loaded += len(verts)
        vbar.value = loaded
        elapsed = time.time() - start
        rate = loaded / elapsed if elapsed else 0
        est_left = (total - loaded) / rate if rate else float('inf')
        vertex_label.value = (
            f"Loaded {loaded} / {total} vertices in {elapsed:.1f}s. "
            f"Est time left: {est_left:.1f}s"
        )

        continuation = attrs.get("x-ms-continuation")
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
