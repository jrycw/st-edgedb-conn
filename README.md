# st-edgedb-conn
This repo showcases my work for [streamlit hackathon](https://discuss.streamlit.io/t/connections-hackathon/47574).

The goal of this repo is to quickly build a connection by extending the built-in `ExperimentalBaseConnection` to execute queries easily on [EdgeDB](https://www.edgedb.com/).

As EdgeDB maintains the official [edgedb-python](https://github.com/edgedb/edgedb-python) client library, our objective is to create the `EdgeDBConnection` class by wrapping the client while adhering to the interface of `ExperimentalBaseConnection`.

The [live demo app](https://st-edgedb-conn.streamlit.app/) is hosted on Streamlit Cloud.
## Basic Concept of EdgeDBConnection 
`edgedb-python` offers 6 types of [query function](https://www.edgedb.com/docs/clients/python/api/blocking_client):
* `client.query`
* `client.query_single`
* `client.query_required_single`
* `client.query_json`
* `client.query_single_json`
* `client.query_required_single_json`

We have abstracted the logic into the `EdgeDBConnection.query` function by utilizing two flags, `jsonify` and `required_single`, respectively.

Below is the relation table for your reference:

| jsonify | required_single | EdgeDB function call |
| -------- | -------- | -------- |
|False     |None      |client.query      |
|False     |False     |client.query_single    |
|False     |True      |client.query_required_single  |
|True      |None      |client.query_json  |
|True      |False     |client.query_single_json     |
|True      |True      |client.query_required_single_json     |

For other operations (`execute`, `transaction`, `close`), we delegate back to the `client` property, which is a nickname for the `_instance` property that wraps our underlying client.

`EdgeDBConnection` follows the context management protocol as it implements `__enter__` and `__exit__` methods, so users can use it with the `with` statement.

Additionally, the instance of `EdgeDBConnection` is callable, serving as a shortcut for executing transactions. Instead of:
```python
    client = edgedb.create_client()
    for tx in client.transaction():
        with tx:
            tx.query('SELECT {1, 2, 3}')
```
Users can simply write:
```python
    conn = EdgeDBConnection()
    for tx in conn():
        tx.query('SELECT {1, 2, 3}')
```
### Limitation
For transaction operations, it seems there is no quick way to inject a custom query function inside it. The object we get from looping through `client.transaction` (`Retry` instance) uses `__slots__`, which prevents us from monkey-patching it. After observing the source code, we found that the transaction method relies on `Retry`, and `Retry.__next__` relies on Iteration. Implementing a custom query function, we would need to carefully rewrite these two classes. The workload for this task does not seem trivial, so we have decided not to focus on this part but to put more effort into coordinating `EdgeDBConnection` with a Streamlit app.

## Installation
### 1. Clone this repo
```
glt clone git@github.com:jrycw/st_edgedb_conn.git
cd st_edgedb_conn
```
### 2. Install the required dependencies:
```
python -m pip install -r requirements.txt 
```
### 3. Setup `EDGEDB_DSN` variable (optional)
_If you don't setup `EDGEDB_DSN` variable, you can directly provide a keyword argument to `EdgeDBConnection`(Check [Usage](#usage) for more detailed information)._

Finally, we need to set the`EDGEDB_DSN` variable in `.streamlit/secrets.toml`.

[`EDGEDB_DSN`](https://www.edgedb.com/docs/reference/dsn#dsn-specification) is the so-called `database-url` for EdgeDB. Establishing a correct DSN can be a bit tricky. Here are some tips:
* To set a user password, you might need this EdgeDB command:
    ```
    edgedb instance reset-password [options] name
    ```
    It will create a JSON file stored at `/home/edgedb/.config/edgedb/credentials/your_instance_name.json` in Linux or `C:\Users\username\AppData\Local\EdgeDB\config\credentials\your_instance_name.json` in Windows. You can try to open this file to find the information you need.

* Each EdgeDB instance can contain multiple databases, and the database itself is part of the DSN. If you don't specify a database parameter, the default `edgedb` will be used.

* `tls_security=insecure` might be a good option if you develop locally or connect to an instance without SSL certification. Use it with caution, though.

The template is as follows:
```toml
[connections.edgedb_conn]
EDGEDB_DSN = "edgedb://edgedb:edgedb@127.0.0.1:10700?database=_example&tls_security=insecure"
```
Please note `edgedb_conn` in the `[connections.edgedb_conn]` must be the same as the first parameter of `EdgeDBConnection`(default to `edgedb_conn`). 

## Usage
### How to create a connection
If you have `EDGEDB_DSN` setup in `.streamlit/secrets.toml`. You can create the connection by:
```python
    conn = EdgeDBConnection()
```

If you have setup a custom connection name, for example "edgedb_connection", in `[connections.edgedb_connection]` in `.streamlit/secrets.toml`, then you need to create the connection by:
```python
    conn = EdgeDBConnection('edgedb_connection')
    conn = EdgeDBConnection(connection_name='edgedb_connection')
```

If you haven't setup `EDGEDB_DSN` in `.streamlit/secrets.toml`. You can create the connection by:
```Python
    conn = EdgeDBConnection(dsn=your_dsn)
    conn = EdgeDBConnection('edgedb_connection', dsn=your_dsn)
    conn = EdgeDBConnection(connection_name='edgedb_connection', dsn=your_dsn)
```

### Create
```python
qry = '''SELECT (INSERT Movie {title:='John Vick 5'}) {title};'''
qry_result = conn.query(create_qry, 
                        required_single=True, 
                        jsonify=True)
```

### Read
```python
qry = '''SELECT assert_single(
                (SELECT Movie {title} 
                 FILTER .title = <str>$title));'''
title = 'John Vick 5'
qry_result = conn.query(qry, 
                        title=title, 
                        required_single=True, 
                        jsonify=True)
```

### Update
```python
qry = '''WITH movie := (SELECT assert_single(
                                (Update Movie
                                 FILTER .title = <str>$title
                                 SET {title := 'John Wick 5'})))
         SELECT movie {title};'''
title = 'John Vick 5'
qry_result = conn.query(qry, 
                        title=title, 
                        required_single=True, 
                        jsonify=True)
```

### Delete
```python
qry = '''WITH movie := (SELECT assert_single(
                                (DELETE Movie
                                 FILTER .title = <str>$title)))
         SELECT movie {title};'''
title = 'John Wick 5'
qry_result = conn.query(qry, 
                        title=title, 
                        required_single=True, 
                        jsonify=True)
```
## UI
Run the following command to open the streamlit web interface in the browser:
```
streamlit run st_app.py
```
Our streamlit application, [st-edgedb-conn](https://st-edgedb-conn.streamlit.app/), is deployed to streamlit cloud as a functional app.

We've recorded a short [video](https://st-edgedb-conn.us-ord-1.linodeobjects.com/videos/st_edgedb_conn_demo.mp4) demonstrating how to perform CRUD operations.

### Handy Utilities
`st-edgedb-conn` offers some handy utilities for users:
* A link to [EdgeDB Cheat Sheet](https://www.edgedb.com/docs/guides/cheatsheet/index).
* A link to [EdgeDB-Python API docs](https://www.edgedb.com/docs/clients/python/api/blocking_client#edgedb-python-blocking-api-reference).
* A link to [Easy EdgeDB](https://www.edgedb.com/easy-edgedb), which is a textbook but more like a story-book.
* `Peek DSN` to display the`EDGEDB_DSN` variable stored in `.streamlit/secrets.toml`.
* `EdgeDB instance healthy check` to display the server status by issuing a HTTP request to the EdgeDB instance.

### Query Form
![Query Form](images/query_form.png)

### Execute Form
![Execute Form](images/execute_form.png)

## Tests
We have some basic tests to validate the `EdgeDBConnection` by utilizing a real EdgeDB instance without mocking.

The tests can be run using:
```
python -m unittest
```

## Caveat
We attempted to use `st.cache_data` to perform the cache operation, but unfortunately, it didn't work as expected.
As a last resort, we utilized `st.cache_resource` to resolve the cache issue.
