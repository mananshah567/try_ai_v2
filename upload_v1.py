from itertools import islice
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

def chunked(iterable, size):
    """Yield successive `size`-sized chunks from `iterable`."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def upload_vertices_in_batches(
    client,
    throttler,
    vertices,
    inserted_vertex_ids,
    batch_size: int = 100
):
    """
    Batch-upload new vertices using Gremlin semicolon-separated scripts.
    - vertices: iterable of dicts with keys 'id', 'label', and optional 'props' (dict).
    - inserted_vertex_ids: set to track existing vertex IDs.
    """
    # filter only those not already inserted
    to_insert = [v for v in vertices if v['id'] not in inserted_vertex_ids]
    total = len(to_insert)

    # progress UI
    label = HTML()
    bar   = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    uploaded = 0
    for batch in chunked(to_insert, batch_size):
        # build a multi-statement Gremlin script
        statements = []
        for v in batch:
            # base addV
            stmt = f"g.addV('{v['label']}').property('id','{v['id']}')"
            # add other properties if provided
            for k, val in v.get('props', {}).items():
                stmt += f".property('{k}','{val}')"
            statements.append(stmt)
        gremlin = "; ".join(statements)

        # submit and throttle
        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        # mark as inserted
        for v in batch:
            inserted_vertex_ids.add(v['id'])

        uploaded += len(batch)
        bar.value = uploaded
        label.value = f"Uploaded {uploaded}/{total} vertices"

    return inserted_vertex_ids


def upload_edges_in_batches(
    client,
    throttler,
    edges,
    inserted_edge_keys,
    batch_size: int = 100
):
    """
    Batch-upload new edges using Gremlin semicolon-separated scripts.
    - edges: iterable of dicts with keys 'out', 'in', 'label', and optional 'props' (dict).
    - inserted_edge_keys: set to track composite keys 'out_label_in'.
    """
    # compose a key and filter
    to_insert = [e for e in edges if f"{e['out']}_{e['label']}_{e['in']}" not in inserted_edge_keys]
    total = len(to_insert)

    label = HTML()
    bar   = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    uploaded = 0
    for batch in chunked(to_insert, batch_size):
        statements = []
        for e in batch:
            stmt = (
                f"g.V('{e['out']}').addE('{e['label']}')"
                f".to(g.V('{e['in']}')).property('id','{e['out']}_{e['label']}_{e['in']}')"
            )
            for k, val in e.get('props', {}).items():
                stmt += f".property('{k}','{val}')"
            statements.append(stmt)
        gremlin = "; ".join(statements)

        rs = client.submit(gremlin)
        attrs = rs.status_attributes
        ru = float(attrs.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru)

        for e in batch:
            inserted_edge_keys.add(f"{e['out']}_{e['label']}_{e['in']}")

        uploaded += len(batch)
        bar.value = uploaded
        label.value = f"Uploaded {uploaded}/{total} edges"

    return inserted_edge_keys

# Example integration in run_graph_upload_checked:
def run_graph_upload_checked(
    client,
    throttler,
    vertices,
    edges,
    inserted_vertex_ids,
    inserted_edge_keys,
    vertex_batch_size: int = 100,
    edge_batch_size:   int = 100
):
    inserted_vertex_ids = upload_vertices_in_batches(
        client, throttler, vertices, inserted_vertex_ids, batch_size=vertex_batch_size
    )
    inserted_edge_keys = upload_edges_in_batches(
        client, throttler, edges, inserted_edge_keys, batch_size=edge_batch_size
    )
    return inserted_vertex_ids, inserted_edge_keys
