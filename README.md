# cat_products

Here's how marriage works: your wife asks for something, and you deliver.

And here it is, a simple web app that allows my wife to search for cat products info from 4 different sites, collected through websraping.

Link to the app: https://jacjaccat.streamlit.app/

Everything is written in Python, something my wife couldn't care less about. 

Dependencies include:

 - httpx: for sending http requests
 - selectorlax: for parsing html
 - playwright: for automating browser
 - polars: for holding the data and some data cleaning
 - duckdb: for persisting the data and some more data cleaning
 - streamlit: for creating the web app
 - streamlit_tags: for the tag components that allow for multiple-keyword search
