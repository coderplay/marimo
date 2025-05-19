# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "polars[pyarrow]==1.27.1",
#     "marimo[sql]",
#     "sqlglot==26.13.0",
#     "requests==2.31.0",
# ]
# ///

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", sql_output="polars")


@app.cell
def init():
    import marimo as mo
    import altair as alt
    import requests, json
    # Create Flink SQL Gateway session
    base_url = "http://localhost:8083"
    response = requests.post(f"{base_url}/v1/sessions")
    flink_session = f"{base_url}/v1/sessions/{response.json()['sessionHandle']}"
    return alt, flink_session, mo


@app.cell
def _(flink_session, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE user_events (
          user_id STRING,
          event_time TIMESTAMP(3),
          WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '50',
          'fields.user_id.length' = '10'
        );
        """,
        engine=flink_session
    )
    return


@app.cell
def _(flink_session, mo, user_events):
    # Execute a simple demo query
    # In a real application, you would use actual tables available in your Flink cluster
    result = mo.sql(
        f"""
        SELECT user_id, COUNT(*) AS cnt FROM user_events GROUP BY user_id;
        """,
        engine=flink_session
    )
    return (result,)


@app.cell
def _(alt, result):
    # Plot the data if available
    if result is not None and not result.is_empty():
        chart = alt.Chart(result).mark_bar().encode(
            x='user_id:N',  # user_id as nominal (categorical) data
            y='cnt:Q'     # count as quantitative data
        ).properties(
            title='User Event Counts',
            width=400,
            height=300
        )
        chart
    return


if __name__ == "__main__":
    app.run()
