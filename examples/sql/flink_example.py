# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "polars[pyarrow]==1.27.1",
#     "marimo[sql]",
#     "sqlglot==26.13.0",
#     "requests==2.31.0",
#     "duckdb==1.2.2",
# ]
# ///

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", sql_output="polars")


@app.cell
def init():
    import marimo as mo
    import requests, json
    # Create Flink SQL Gateway session
    base_url = "http://localhost:8083"
    response = requests.post(f"{base_url}/v1/sessions")
    flink_session = f"{base_url}/v1/sessions/{response.json()['sessionHandle']}"
    return flink_session, mo


@app.cell
def _(flink_session, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE person (
          id BIGINT,
          name STRING,
          email STRING,
          city STRING,
          ts TIMESTAMP(3),
          WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '10',
          'fields.id.kind' = 'sequence',
          'fields.id.start' = '1',
          'fields.id.end' = '10000',
          'fields.name.length' = '10',
          'fields.email.length' = '15',
          'fields.city.length' = '8'
        );
        """,
        engine=flink_session
    )
    return


@app.cell
def _(flink_session, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE auction (
          id BIGINT,
          seller_id BIGINT,
          category_id INT,
          reserve DOUBLE,
          item_name STRING,
          ts TIMESTAMP(3),
          WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '20',
          'fields.id.kind' = 'sequence',
          'fields.id.start' = '1',
          'fields.id.end' = '100000',
          'fields.seller_id.min' = '1',
          'fields.seller_id.max' = '10000',
          'fields.category_id.min' = '1',
          'fields.category_id.max' = '100',
          'fields.reserve.min' = '10',
          'fields.reserve.max' = '1000',
          'fields.item_name.length' = '12'
        );
        """,
        engine=flink_session
    )
    return


@app.cell
def _(flink_session, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE bid (
          auction_id BIGINT,
          bidder_id BIGINT,
          price DOUBLE,
          ts TIMESTAMP(3),
          WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '50',
          'fields.auction_id.min' = '1',
          'fields.auction_id.max' = '100000',
          'fields.bidder_id.min' = '1',
          'fields.bidder_id.max' = '10000',
          'fields.price.min' = '1',
          'fields.price.max' = '5000'
        );
        """,
        engine=flink_session
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Example Nexmark-like Query: Max Bid Per Auction Window""")
    return


@app.cell
def _(bid, flink_session, mo):
    max_bids = mo.sql(
        f"""
        SELECT
          auction_id,
          TUMBLE_START(ts, INTERVAL '1' SECOND) AS window_start,
          MAX(price) AS max_price
        FROM bid
        GROUP BY
          auction_id,
          TUMBLE(ts, INTERVAL '1' SECOND);
        """,
        engine=flink_session
    )
    return (max_bids,)


@app.cell
def _(max_bids):
    max_bids
    return


@app.cell
def _(max_bids, mo):
    _df = mo.sql(
        f"""
        SELECT window_start, max_price FROM max_bids where auction_id = 22265
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Most Active Bidders (Top N by Count)""")
    return


@app.cell
def _(bid, flink_session, mo):
    _df = mo.sql(
        f"""
        SELECT bidder_id, COUNT(*) AS total_bids
        FROM bid
        GROUP BY bidder_id
        ORDER BY total_bids DESC
        LIMIT 50;
        """,
        engine=flink_session
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Number of Bids per Auction in Tumbling Windows""")
    return


@app.cell
def _(bid, flink_session, mo):
    _df = mo.sql(
        f"""
        SELECT
          auction_id,
          TUMBLE_START(ts, INTERVAL '10' SECOND) AS window_start,
          COUNT(*) AS bid_count
        FROM bid
        GROUP BY
          auction_id,
          TUMBLE(ts, INTERVAL '10' SECOND);
        """,
        engine=flink_session
    )
    return


if __name__ == "__main__":
    app.run()
