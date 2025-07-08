import time
import itertools
from gremlin_python.driver.client import Client
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def chunked(iterable, size):
    """Yield successive `size`‐sized chunks from `iterable`."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk

def load_edges_in_vertex_batches(
    client: Client,
    throttler,
    vertex_ids,
    vertex_batch_size: int = 1_000,
    edge_page_size:   int = 500
):
    """
    Load all edges by scanning `bothE()` over batches of vertices.
    Each batch is paged by continuation tokens with `pageSize=edge_page_size`.
    """
    inserted = set()
    total_vertices = len(vertex_ids)
    v_label = HTML()
    v_bar   = IntProgress(min=0, max=total_vertices, description="Vertices:")
    display(VBox([v_label, v_bar]))

    processed_v = 0
    start = time.time()

    # this Gremlin will return *ALL* edges incident on any of the ids in 'ids'
    gremlin = """
      g.V(ids)
       .bothE()
       .project('out','label','in')
         .by(outV().id())
         .by(label())
         .by(inV().id())
    """

    for batch in chunked(vertex_ids, vertex_batch_size):
        continuation = None
        # page through this batch’s edges
        while True:
            rs = client.submit(
                gremlin,
                {"ids": batch},
                request_options={
                  "continuation": continuation,
                  "pageSize": edge_page_size
                }
            )
            edges = rs.all().result()
            attrs = rs.status_attributes

            # RU throttle
            ru = float(attrs.get("x-ms-total-request-charge", 10.0))
            throttler.throttle(ru)

            # collect
            for e in edges:
                inserted.add(f"{e['out']}_{e['label']}_{e['in']}")

            continuation = attrs.get("x-ms-continuation")
            if not continuation:
                break

        # update vertex‐batch progress
        processed_v += len(batch)
        v_bar.value = processed_v
        elapsed = time.time() - start
        rate = processed_v / elapsed if elapsed else 0
        eta  = (total_vertices - processed_v) / rate if rate else float('inf')
        v_label.value = (
            f"Processed {processed_v}/{total_vertices} vertices in {elapsed:.1f}s; "
            f"ETA {eta:.1f}s"
        )

    return inserted
