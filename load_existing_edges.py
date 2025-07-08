import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from ipywidgets import IntProgress, HTML, VBox
from IPython.display import display

# — Optional — create your client with a higher read timeout
client = Client(
    'wss://<YOUR_ACCOUNT>.gremlin.cosmos.azure.com:443/',
    'g',
    username='/dbs/<DB>/colls/<GRAPH>',
    password='<PRIMARY_KEY>',
    message_serializer=serializer.GraphSONSerializersV2d0(),
    # bump to 10 minutes
    transport_factory=lambda: AiohttpTransport(read_timeout=600_000, write_timeout=600_000)
)

def load_existing_edges(client: Client, throttler, batch_size: int = 500, max_retries: int = 3):
    """
    Load all existing edges via continuation tokens, with progress bar,
    retrying on GraphTimeoutException and optionally backing off page_size.
    Returns a set of composite keys "out_label_in".
    """
    # — 1) Try to get total count (non-chunked, so may time out) ——
    try:
        total = client.submit("g.E().count()", {}, request_options={"pageSize": 1}) \
                      .all().result()[0]
    except GremlinServerError as e:
        print("Could not retrieve total edge count (timed out). Progress bar will be indeterminate.")
        total = None

    # Set up progress bar if we got a count
    if total is not None:
        edge_label = HTML()
        ebar = IntProgress(min=0, max=total, description="Edges:")
        display(VBox([edge_label, ebar]))
    else:
        edge_label = None
        ebar = None

    continuation = None
    inserted_edge_keys = set()
    loaded = 0
    start = time.time()

    query = """
      g.E()
       .project('out','label','in')
         .by(outV().id())
         .by(label())
         .by(inV().id())
    """

    current_page_size = batch_size

    while True:
        # — 2) Attempt each page with retry on timeout ——
        for attempt in range(1, max_retries+1):
            try:
                result_set = client.submit(
                    query,
                    {},
                    request_options={
                        "continuation": continuation,
                        "pageSize": current_page_size
                    }
                )
                break
            except GremlinServerError as e:
                if "GraphTimeoutException" in str(e) and attempt < max_retries:
                    print(f"Page {loaded}–{loaded+current_page_size} timed out; retry {attempt}/{max_retries}…")
                    # back off to a smaller page
                    current_page_size = max(10, current_page_size // 2)
                    time.sleep(1)
                else:
                    raise

        # process the successful page
        edges = result_set.all().result()
        attrs = result_set.status_attributes

        # throttle by RU
        ru = float(attrs.get("x-ms-total-request-charge", 10.0))
        throttler.throttle(ru)

        for e in edges:
            key = f"{e['out']}_{e['label']}_{e['in']}"
            inserted_edge_keys.add(key)

        loaded += len(edges)
        continuation = attrs.get("x-ms-continuation")

        # update progress bar/label
        if ebar:
            ebar.value = loaded
            elapsed = time.time() - start
            rate = loaded / elapsed if elapsed else 0
            est_left = (total - loaded) / rate if (total and rate) else float('inf')
            edge_label.value = (
                f"Loaded {loaded:,} / {total:,} edges in {elapsed:.1f}s; "
                f"ETA {est_left:.1f}s"
            )

        if not continuation:
            break

    return inserted_edge_keys
