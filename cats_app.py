import duckdb
import polars as pl
import streamlit as st
from streamlit_tags import st_tags

DB = "cat_products.db"

def connect_db(db: str) -> duckdb.duckdb.DuckDBPyConnection:
    """
    :param db: duckdb database string
    :return: duckdb database connection
    """
    return duckdb.connect(db)

def get_data_set(conn: duckdb.duckdb.DuckDBPyConnection) -> pl.dataframe.frame.DataFrame:
    """
    :param conn: duckdb database connect
    :return: a polars result set that contains all of the items
    """
    result = conn.execute(""" 
        select
            sitenames
            , names
            , weights
            , prices
            , extra_info
            , links
        from
            v_all_cat_products
    """).pl()

    return result

def get_site_names(conn: duckdb.duckdb.DuckDBPyConnection) -> list[str]:
    """
    :param conn: duckdb database connection
    :return: a list that contains the names of the sites availble
    """
    sites = conn.execute("""
        select 
            distinct sitenames
        from 
            v_all_cat_products
    """).fetchall()

    return [site[0] for site in sites]

def get_final_results(
    data: pl.dataframe.frame.DataFrame,
    keywords_and: list[str],
    site_options: list[str],
) -> pl.dataframe.frame.DataFrame:
    """
    :param data: a polar dataframe that contains the current items
    :param keywords_and: a list of zero or more keywords as currently entered by the user
    :param site_options: a list of zero ore more sitenames as currently selected by the user
    :return: a polar dataframe filtered by the selected keywords and sitenames
    """
    with_sites = data.filter(pl.col("sitenames").is_in(site_options))
    if not keywords_and:
        return with_sites
    else:
        for kw in keywords_and:
            kw = kw.lower()
            with_sites = with_sites.filter(pl.col("names").str.to_lowercase().str.contains(kw))
        return with_sites

def main():
    conn = connect_db(db=DB)
    data_set = get_data_set(conn=conn)
    sites = get_site_names(conn=conn)

    st.set_page_config(
        page_title="Search Cats Products",
        layout="wide"
    )

    st.header('🐱🐱Search Cats Products🐱🐱', divider='rainbow')

    with st.chat_message("assistant"):
        st.write("Choose the sites ⬇️⬇️⬇️")
        site_options = st.multiselect(
            label="Search site name",
            options=sites,
            default=sites,
            label_visibility='collapsed'
        )


    with st.chat_message("assistant"):
        st.write("Customize your search ⬇️⬇️⬇️")

        keywords_and = st_tags(
            label="Enter Keywords:",
            text="and...",
            maxtags=20,
            key=1
        )

    st.divider()

    result = get_final_results(
        data=data_set,
        keywords_and=keywords_and,
        site_options=site_options
    )

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
    result_msg.write("Result ⬇️⬇️⬇️")

    st.dataframe(
        result,
        width=8000,
        column_config={
            "names": st.column_config.TextColumn(width="large"),
            "links": st.column_config.LinkColumn("links")
        }
    )

    st.divider()

    with st.chat_message("assistant"):
        st.write("Number of items found from each site ⬇️⬇️⬇️")

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
