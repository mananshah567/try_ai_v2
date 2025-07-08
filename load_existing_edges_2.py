import time
from gremlin_python.driver.client import Client
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def load_edges_by_vertex(client: Client, throttler, vertex_ids, batch_size: int = 500):
    """
    Instead of g.E(), loop over each vertex and page through its edges.
    This keeps each traversal tiny and avoids server timeouts.
    Returns a set of composite keys "out_label_in".
    """
    inserted = set()
    total_v = len(vertex_ids)

    # outer progress bar (vertices processed)
    vlabel = HTML()
    vbar   = IntProgress(min=0, max=total_v, description="Vertices:")
    display(VBox([vlabel, vbar]))

    start = time.time()
    processed = 0

    # Gremlin snippet to get all edges incident to a single vertex
    per_vertex_query = """
      g.V(vertexId).bothE()
        .project('out','label','in')
          .by(outV().id())
          .by(label())
          .by(inV().id())
    """

    for vid in vertex_ids:
        continuation = None
        while True:
            result_set = client.submit(
                per_vertex_query,
                {"vertexId": vid},
                request_options={"continuation": continuation, "pageSize": batch_size}
            )
            edges = result_set.all().result()
            attrs = result_set.status_attributes

            # RU throttle
            ru = float(attrs.get("x-ms-total-request-charge", 10.0))
            throttler.throttle(ru)

            # record each edge
            for e in edges:
                key = f"{e['out']}_{e['label']}_{e['in']}"
                inserted.add(key)

            continuation = attrs.get("x-ms-continuation")
            if not continuation:
                break

        processed += 1
        vbar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta  = (total_v - processed) / rate if rate else float('inf')
        vlabel.value = (
            f"Vertices processed: {processed}/{total_v} "
            f"in {elapsed:.1f}s (ETA {eta:.1f}s)"
        )

    return inserted
