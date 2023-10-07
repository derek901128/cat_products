import duckdb
import polars as pl
import streamlit as st

DB = "cat_products.db"

def connect_db(db: str):
    return duckdb.connect(db)

def get_data_set(conn):
    result = conn.execute(""" 
        select
            sitenames
            , names
            , weights
            , prices
            , extra_info
            , last_updated
            , links
        from
            v_all_cat_products
    """).pl()

    return result

def get_site_names(conn):
    sites = conn.execute("""
        select 
            distinct sitenames
        from 
            v_all_cat_products
    """).fetchall()

    return [site[0] for site in sites]

def main():
    conn = connect_db(db=DB)
    data_set = get_data_set(conn=conn)
    sites = get_site_names(conn=conn)

    st.set_page_config(
        page_title="Search Cats Products",
        layout="wide"
    )

    st.header('🐱🐱Search Cats Products🐱🐱', divider='rainbow')

    site_options = st.multiselect(
        label="Search site name",
        options=sites,
        default=sites
    )

    name_search = st.text_input(
        label="Search product name",
        placeholder="Enter product name"
    )

    st.divider()

    result = data_set.filter(
        (pl.col("names").str.contains(name_search)) & (pl.col("sitenames").is_in(site_options))
    ) if name_search else data_set.filter(pl.col("sitenames").is_in(site_options))

    try:
        count_1 = result.group_by("sitenames").agg(pl.count("names").alias("count")).filter(pl.col("sitenames").str.contains("卷卷巿集"))[0, 1]
    except:
        count_1 = 0
    try:
        count_2 = result.group_by("sitenames").agg(pl.count("names").alias("count")).filter(pl.col("sitenames").str.contains("Cat is Cat"))[0, 1]
    except:
        count_2 = 0
    try:
        count_3 = result.group_by("sitenames").agg(pl.count("names").alias("count")).filter(pl.col("sitenames").str.contains("皮皮"))[0, 1]
    except:
        count_3 = 0
    try:
        count_4 = result.group_by("sitenames").agg(pl.count("names").alias("count")).filter(pl.col("sitenames").str.contains("Super Pet"))[0, 1]
    except:
        count_4 = 0

    result_msg = st.chat_message("assistant")
    result_msg.write("Result below ⬇️⬇️⬇️")

    st.dataframe(
        result,
        column_config={
            "links": st.column_config.LinkColumn("links")
        }
    )

    st.divider()

    metrics_msg = st.chat_message("assistant")
    metrics_msg.write("Count from each site below ⬇️⬇️⬇️")

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)

    with metric_1:
        st.metric(
            label="卷卷巿集",
            value=count_1
        )

    with metric_2:
        st.metric(
            label="Cat is Cat",
            value=count_2
        )

    with metric_3:
        st.metric(
            label="皮皮",
            value=count_3
        )

    with metric_4:
        st.metric(
            label="Super Pet",
            value=count_4
        )

if __name__ == "__main__":
    main()


