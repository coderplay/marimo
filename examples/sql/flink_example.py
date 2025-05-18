# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "polars[pyarrow]==1.27.1",
#     "marimo[sql]",
# ]
# ///

import marimo

__generated_with = "0.13.10"
app = marimo.App(width="medium", sql_output="polars")


@app.cell
def _():
    import marimo as mo
    flink_engine = "http://localhost:8083"
    return flink_engine, mo


@app.cell
def _(flink_engine, mo):
    # Check available databases
    mo.md("### Available Databases")
    try:
        databases = mo.sql("SHOW DATABASES", engine=flink_engine)
        if databases is not None and len(databases) > 0:
            mo.md(f"Found {len(databases)} databases")
        else:
            mo.md("No databases found or could not connect to Flink SQL Gateway")
    except Exception as e:
        mo.md(f"Error connecting to Flink SQL Gateway: {str(e)}")
    return


@app.cell
def _(flink_engine, mo):
    # Execute a simple demo query
    # In a real application, you would use actual tables available in your Flink cluster
    result = mo.sql(
        f"""
        -- This simulates data - replace with actual tables when connected to a Flink cluster
        SELECT 
            CAST(value AS INT) as id,
            CAST(value * 2 AS INT) as doubled_value
        FROM (
            VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
        ) AS T(value)
        """,
        engine=flink_engine
    )
    return (result,)


@app.cell
def _(alt, result):
    # Plot the data if available
    if result is not None and not result.is_empty():
        chart = alt.Chart(result).mark_bar().encode(
            x='id:O',
            y='doubled_value:Q'
        ).properties(
            title='Sample Flink SQL Result'
        )
        chart
    return


if __name__ == "__main__":
    app.run()
