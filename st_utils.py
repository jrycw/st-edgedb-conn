import base64
from pathlib import Path


def render_png(png):
    """https://stackoverflow.com/questions/70932538/how-to-center-the-title-and-an-image-in-streamlit"""
    img_bytes = Path(png).read_bytes()
    b64 = base64.b64encode(img_bytes).decode()
    return f'''<div style="text-align:center;">
               <img src="data:image/png;base64,{b64}"/></div>'''


def render_svg(svg):
    """https://gist.github.com/treuille/8b9cbfec270f7cda44c5fc398361b3b1"""
    b64 = base64.b64encode(svg.encode('utf-8')).decode("utf-8")
    return f'''<div style="text-align:center;">
            <img src="data:image/svg+xml;base64,{b64}"/></div>'''


def required_single_format_func(option):
    match option:
        case None:   # "qurey_(json)"
            return f'{str(option)}  : The query will just return whatever it got.'
        case False:  # "qurey_single_(json)"
            return f'{str(option)} : The query must return no more than one element.'
        case True:   # "qurey_required_single_(json)"
            return f'{str(option)} : The query must return exactly one element.'


edgedb_intro = '''
    [EdgeDB](https://www.edgedb.com/) is an open-source database designed as a 
    spiritual successor to SQL and the relational paradigm. It aims to solve 
    some hard design problems that make existing databases unnecessarily 
    onerous to use.
    
    #### PROTOCOL
    EdgeDB's binary protocol is designed to have minimal latency possible, 
    fast data marshalling, and to survive network errors.
    
    #### FULLY OPEN-SOURCE
    EdgeDB is 100% Open Source and is distributed under Apache 2.0 license, 
    including the database core, the client libraries, the CLI, and many other upcoming 
    libraries and services.

    #### OPTIMIZING COMPILER
    EdgeDB is analagous to "LLVM for data", in that it compiles its high-level schema 
    and queries to low-level tables and optimized SQL. The result is great performance 
    without sacrificing usability or functionality.

    #### INTEGRATION PLATFORM
    EdgeDB goes well beyond a typical feature set of a database. Features that are 
    traditionally pushed to clients to deal with (HTTP API, GraphQL, observability) are 
    implemented right in the core.
    
    #### BUILT ON POSTGRESQL
    EdgeDB uses PostgreSQL as its data storage and query execution engine, benefitting 
    from its exceptional reliability.
    
    #### FAST QUERIES
    Convenience usually comes at the price of performance, but not this time. EdgeQL is 
    compiled into Postgres queries that go head-to-head with the best handwritten SQL.
    
    #### ASYNCHRONOUS CORE
    EdgeDB server utilizes non-blocking IO to make client connections cheap and 
    scalable, solving the connection overload problem that's increasingly prevalent in 
    an auto-scaling, serverless deployments. The underlying PostgreSQL connection pool 
    will be automatically scaled as needed.
    '''

param_fun_relations = [('False', 'None', 'client.query'),
                       ('False', 'False', 'client.query_single'),
                       ('False', 'True', 'client.query_required_single'),
                       ('True',  'None', 'client.query_json'),
                       ('True',  'False', 'client.query_single_json'),
                       ('True',  'True', 'client.query_required_single_json'),]
