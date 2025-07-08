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
    vertex_pk_id_pairs,          # list of [partitionKey, vertexId]
    vertex_batch_size: int = 1_000,
    edge_page_size:   int = 500
):
    """
    Load all edges by scanning `bothE()` over batches of vertices—using composite keys.
    vertex_pk_id_pairs: e.g. [ ['USA','v1'], ['CAN','v2'], … ]
    Each batch is paged by continuation tokens with `pageSize=edge_page_size`.
    Returns a set of "out_label_in" strings.
    """
    inserted = set()
    total_vertices = len(vertex_pk_id_pairs)

    # Progress bar for vertices
    v_label = HTML()
    v_bar   = IntProgress(min=0, max=total_vertices, description="Vertices:")
    display(VBox([v_label, v_bar]))

    processed = 0
    start = time.time()

    gremlin = """
      g.V(ids)
       .bothE()
       .project('out','label','in')
         .by(outV().id())
         .by(label())
         .by(inV().id())
    """

    for batch in chunked(vertex_pk_id_pairs, vertex_batch_size):
        continuation = None

        # page through edges for this batch
        while True:
            rs = client.submit(
                gremlin,
                { "ids": batch },
                request_options={
                    "continuation": continuation,
                    "pageSize": edge_page_size
                }
            )
            edges = rs.all().result()
            attrs = rs.status_attributes

            # throttle on RU
            ru = float(attrs.get("x-ms-total-request-charge", 10.0))
            throttler.throttle(ru)

            # collect edges
            for e in edges:
                inserted.add(f"{e['out']}_{e['label']}_{e['in']}")

            continuation = attrs.get("x-ms-continuation")
            if not continuation:
                break

        # update vertex‐batch progress
        processed += len(batch)
        v_bar.value = processed
        elapsed = time.time() - start
        rate = processed / elapsed if elapsed else 0
        eta  = (total_vertices - processed) / rate if rate else float('inf')
        v_label.value = (
            f"Processed {processed}/{total_vertices} vertices in {elapsed:.1f}s; "
            f"ETA {eta:.1f}s"
        )

    return inserted
