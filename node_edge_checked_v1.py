def upload_vertices_checked(client, df_vertices, throttler):
    progress = IntProgress(min=0, max=len(df_vertices), description='Checked V:')
    label = HTML()
    display(VBox([label, progress]))

    for i, row in df_vertices.iterrows():
        progress.value += 1
        label.value = f"Checked insert vertex {progress.value} / {len(df_vertices)}"

        vertex_id = str(row["ID"])
        label_val = str(row["Label"])

        check_query = f"g.V('{vertex_id}').hasNext()"
        exists = False
        try:
            result = client.submit(check_query)
            response = result.all()
            exists = bool(response.result())
            status_attrs = result.status_attributes
            if 'x-ms-total-request-charge' in status_attrs:
                ru = float(status_attrs.get('x-ms-total-request-charge', 10.0))
                throttler.throttle(ru)
        except Exception as e:
            logger.error(f"Vertex check failed {vertex_id}: {e}")

        if exists:
            continue

        query = f"g.addV('{label_val}').property('id', '{vertex_id}')"
        prop_fn = ENTITY_PROPERTY_DISPATCHER.get(label_val)
        if prop_fn:
            query = prop_fn(row, query)

        try:
            result = client.submit(query)
            response = result.all()
            status_attrs = result.status_attributes
            if 'x-ms-total-request-charge' in status_attrs:
                ru = float(status_attrs['x-ms-total-request-charge'])
                throttler.throttle(ru)
        except GremlinServerError as e:
            logger.error(f"Checked vertex insert error {vertex_id}: {e}")

# --------------------------------------
# Checked Upload Edges
# --------------------------------------
def upload_edges_checked(client, df_edges, throttler):
    progress = IntProgress(min=0, max=len(df_edges), description='Checked E:')
    label = HTML()
    display(VBox([label, progress]))

    for i, row in df_edges.iterrows():
        progress.value += 1
        label.value = f"Checked insert edge {progress.value} / {len(df_edges)}"

        out_id = str(row["OutV"])
        in_id = str(row["InV"])
        label_val = str(row["Label"])
        edge_key = f"{out_id}-{label_val}->{in_id}"

        check_query = f"g.V('{out_id}').outE('{label_val}').as('e').inV().hasId('{in_id}').select('e').hasNext()"
        exists = False
        try:
            result = client.submit(check_query)
            response = result.all()
            exists = bool(response.result())
            status_attrs = result.status_attributes
            if 'x-ms-total-request-charge' in status_attrs:
                ru = float(status_attrs['x-ms-total-request-charge'])
                throttler.throttle(ru)
        except Exception as e:
            logger.error(f"Edge check failed {edge_key}: {e}")

        if exists:
            continue

        query = f"g.V('{out_id}').addE('{label_val}').to(g.V('{in_id}'))"
        for col in df_edges.columns:
            if col in ["OutV", "InV", "Label"] or pd.isna(row[col]):
                continue
            val = row[col]
            if isinstance(val, str):
                query += f".property('{col}', '{escape_gremlin_str(val)}')"
            else:
                query += f".property('{col}', {val})"

        try:
            result = client.submit(query)
            response = result.all()
            status_attrs = result.status_attributes
            if 'x-ms-total-request-charge' in status_attrs:
                ru = float(status_attrs['x-ms-total-request-charge'])
                throttler.throttle(ru)
        except GremlinServerError as e:
            logger.error(f"Checked edge insert error {edge_key}: {e}")

# --------------------------------------
# Run Graph Upload Checked
# --------------------------------------
def run_graph_upload_checked(df_vertices, df_edges):
    throttler = RUThrottler(RU_LIMIT_PER_SECOND)
    upload_vertices_checked(client, df_vertices, throttler)
    upload_edges_checked(client, df_edges, throttler)
