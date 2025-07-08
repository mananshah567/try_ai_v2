import time
import itertools
from gremlin_python.driver.client import Client
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display


def chunked(iterable, size):
    """Yield successive `size`-sized chunks from `iterable`."""
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            return
        yield batch


def upload_vertices_with_existence_check(
    client: Client,
    throttler,
    vertices,
    batch_size: int = 100
):
    """
    Batch-upload vertices, checking existence on the fly via `has('id', within(...))`.
    Returns the number of vertices actually inserted.

    - `vertices`: iterable of dicts with keys 'id', 'label', optional 'props'.
    - `batch_size`: how many vertices to check/insert per batch.
    """
    total = len(vertices)
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Vertices:")
    display(VBox([label, bar]))

    processed = 0
    uploaded = 0
    start = time.time()

    for batch in chunked(vertices, batch_size):
        # build literal list of ids for existence check
        ids_literal = ",".join(f"'{v['id']}'" for v in batch)
        check_query = f"g.V().has('id', within({ids_literal})).id()"
        rs_chk = client.submit(check_query)
        existing = set(rs_chk.all().result())
        ru_chk = float(rs_chk.status_attributes.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru_chk)

        # filter those not yet in graph
        to_insert = [v for v in batch if v['id'] not in existing]
        if to_insert:
            statements = []
            for v in to_insert:
                stmt = f"g.addV('{v['label']}').property('id','{v['id']}')"
                for k, val in v.get('props', {}).items():
                    stmt += f".property('{k}','{val}')"
                statements.append(stmt)
            gremlin = "; ".join(statements)
            rs_ins = client.submit(gremlin)
            ru_ins = float(rs_ins.status_attributes.get('x-ms-total-request-charge', 10.0))
            throttler.throttle(ru_ins)
            uploaded += len(to_insert)

        processed += len(batch)
        bar.value = processed
        elapsed = time.time() - start
        label.value = (
            f"Processed {processed}/{total} vertices, "
            f"Inserted {uploaded}, elapsed {elapsed:.1f}s"
        )

    return uploaded


def upload_edges_with_existence_check(
    client: Client,
    throttler,
    edges,
    batch_size: int = 100
):
    """
    Batch-upload edges, checking existence on the fly via `has('id', within(...))`.
    Returns the number of edges actually inserted.

    - `edges`: iterable of dicts with keys 'out', 'in', 'label', optional 'props'.
    - `batch_size`: how many edges to check/insert per batch.
    """
    total = len(edges)
    label = HTML()
    bar = IntProgress(min=0, max=total, description="Edges:")
    display(VBox([label, bar]))

    processed = 0
    uploaded = 0
    start = time.time()

    for batch in chunked(edges, batch_size):
        # build composite key list
        keys = [f"{e['out']}_{e['label']}_{e['in']}" for e in batch]
        keys_literal = ",".join(f"'{k}'" for k in keys)
        check_query = f"g.E().has('id', within({keys_literal})).id()"
        rs_chk = client.submit(check_query)
        existing = set(rs_chk.all().result())
        ru_chk = float(rs_chk.status_attributes.get('x-ms-total-request-charge', 10.0))
        throttler.throttle(ru_chk)

        # filter edges not yet in graph
        to_insert = [e for e, key in zip(batch, keys) if key not in existing]
        if to_insert:
            statements = []
            for e in to_insert:
                key = f"{e['out']}_{e['label']}_{e['in']}"
                stmt = (
                    f"g.V('{e['out']}').addE('{e['label']}')"
                    f".to(g.V('{e['in']}')).property('id','{key}')"
                )
                for k, val in e.get('props', {}).items():
                    stmt += f".property('{k}','{val}')"
                statements.append(stmt)
            gremlin = "; ".join(statements)
            rs_ins = client.submit(gremlin)
            ru_ins = float(rs_ins.status_attributes.get('x-ms-total-request-charge', 10.0))
            throttler.throttle(ru_ins)
            uploaded += len(to_insert)

        processed += len(batch)
        bar.value = processed
        elapsed = time.time() - start
        label.value = (
            f"Processed {processed}/{total} edges, "
            f"Inserted {uploaded}, elapsed {elapsed:.1f}s"
        )

    return uploaded
