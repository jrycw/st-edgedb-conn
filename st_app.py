import datetime

import pandas as pd
import streamlit as st

from src.st_edgedb_conn import EdgeDBConnection
from st_utils import (
    edgedb_intro,
    param_fun_relations,
    render_png,
    render_svg,
    required_single_format_func,
)

st.set_page_config(
    page_title='Streamlit EdgeDB Connection',
    layout='centered')


with st.sidebar:
    st.write(render_png('images/edb_logo_green.png'), unsafe_allow_html=True)
    st.write(edgedb_intro)


def main():
    conn = EdgeDBConnection()
    dsn = conn._dsn

    with st.container():
        st.markdown('#### Handy utilities')
        cheet_sheet_col, api_doc_col, easy_edgedb_col = st.columns([1, 1, 1])
        with cheet_sheet_col:
            st.markdown(
                '[EdgeDB Cheat Sheet](https://www.edgedb.com/docs/guides/cheatsheet/index)')
        with api_doc_col:
            st.markdown(
                '[EdgeDB-Python API docs](https://www.edgedb.com/docs/clients/python/api/blocking_client#edgedb-python-blocking-api-reference)')
        with easy_edgedb_col:
            st.markdown('[Easy EdgeDB](https://www.edgedb.com/easy-edgedb)')

        dsn_col, healthy_col = st.columns([1,  2])

        with dsn_col:
            if st.button('Peek DSN'):
                st.toast(dsn, icon="✅")

        with healthy_col:
            if st.button('EdgeDB instance healthy check'):
                if conn.is_healthy():
                    st.toast('Connected successfully', icon="✅")
                else:
                    st.toast('Connected unsuccessfully', icon="🚨")

    query_tab, exec_tab, benchmark_tab = st.tabs(['Query Form',
                                                  'Exec Form',
                                                  'Performance Benchmarks'])

    with query_tab:
        with st.form('query-form'):
            with st.expander('''Find the corresponding EdgeDB function call by referring
                              to `jsonify` & `required_single`'''):
                df = pd.DataFrame(param_fun_relations, columns=[
                    'jsonify', 'required_single', 'EdgeDB function call'])
                st.dataframe(df, hide_index=True)

            create_snip_tab, read_snip_tab, update_snip_tab, delete_snip_tab = st.tabs(
                ['Create', 'Read', 'Update', 'Delete'])

            with create_snip_tab:
                st.code(
                    '''SELECT (INSERT Movie {title := 'John Wick 05'}) {title};''')

            with read_snip_tab:
                st.code(
                    '''SELECT Movie {title} FILTER .title = <str>$title;''')
                st.code('''SELECT Movie {title} FILTER .title = <str>$0;''')
                st.code(
                    '''SELECT assert_single((SELECT Movie {title} FILTER .title = <str>$title));''')

            with update_snip_tab:
                st.code('''WITH movie := (SELECT assert_single(
                            (UPDATE Movie
                             FILTER .title = <str>$title
                             SET {title := 'John Wick 5'})))\nSELECT movie {title};''')

                st.code(
                    '''SELECT (UPDATE Movie FILTER .title = <str>$title SET {title := 'John Wick 5'}) {title};''')

            with delete_snip_tab:
                st.code('''WITH movie := (SELECT assert_single(
                            (DELETE Movie
                             FILTER .title = <str>$title)))\nSELECT movie {title};''')
                st.code(
                    '''SELECT (DELETE Movie FILTER .title = <str>$title) {title};''')

            qry = st.text_area(
                'EdgeDB Query', placeholder='Example: \nSELECT Movie {title};')

            with st.container():
                st.caption(
                    '`str`, `datetime.date` and `datetime.datetime` object are ' +
                    'supported for positional and query arguments.')
                args_col, kwargs_col = st.columns(2)
                with args_col:
                    qry_args_str = st.text_area(
                        'Positional query arguments (separated by semicolon)',
                        placeholder="Example: \n1; 2.5; 'Continental Hotel'; " +
                                    "datetime.date(1964, 9, 2)")

                with kwargs_col:
                    qry_kwargs_str = st.text_area(
                        'Named query arguments (separated by semicolon)',
                        placeholder="Example: \ntitle='John Wick 5';" +
                        " name='Keanu Charles Reeves';" +
                        " birthday=datetime.date(1964, 9, 2)")

            with st.container():
                required_single_col, ttl_jsonify_col = st.columns([1.8, 1])
                with required_single_col:
                    required_single = st.radio('required_single?',
                                               (None, False, True),
                                               index=0,
                                               format_func=required_single_format_func,
                                               help='Refer to the table above to ' +
                                               'identify the corresponding EdgeDB ' +
                                               'function being called.')

                with ttl_jsonify_col:
                    ttl = st.slider('ttl (secs), for `READ` operation only',
                                    min_value=-1,
                                    max_value=60,
                                    value=-1,
                                    step=1,
                                    help='ttl=-1 indicates that the cache will never ' +
                                    'expire (default behavior).\n\n' +
                                    'ttl=0 means there is no cache at all.\n\n')
                    ttl = ttl if ttl >= 0 else None

                    jsonify = st.checkbox('Jsonify',
                                          value=True,
                                          help='jsonify provides better visibility')

            with st.expander('Use cache with caution.'):
                st.write('''For `READ` operations, everything should work smoothly. 
                            However, it should be noted that the database could 
                            possibly perform `CREATE`, `UPDATE`, or `DELETE` 
                            operations by other connections or drivers. Under these
                            circumstances, consider setting a low cache value to avoid
                            unexpected outcomes. Also, please keep in mind that in our 
                            app, caching is only applicable to read operations and will
                            not be activated for `CREATE`, `UPDATE`, or `DELETE` 
                            operations, even if you set up the `ttl` value.''')

            *_, qry_last_col = st.columns(7)
            with qry_last_col:
                qry_submit_button = st.form_submit_button('Query')

            if qry_submit_button:
                jsonify = True if jsonify else False

                qry_args = []
                for arg in qry_args_str.split(';'):
                    if arg.strip() and \
                            isinstance(arg, (str, datetime.date, datetime.datetime)):
                        try:
                            qry_args.append(eval(arg))
                        except SyntaxError as e:
                            st.warning(
                                'Can not parse the positional query arguments!')
                            raise e

                qry_kwargs = {}
                for row in qry_kwargs_str.split(';'):
                    try:
                        exec(row.strip(), globals(), qry_kwargs)
                    except SyntaxError as e:
                        st.warning('Can not parse the named query arguments!')
                        raise e

                qry_result = conn.query(qry,
                                        *qry_args,
                                        ttl=ttl,
                                        jsonify=jsonify,
                                        required_single=required_single,
                                        **qry_kwargs)

                with st.container():
                    st.markdown('#### Query result: ')
                    if jsonify:
                        if (null := str(qry_result)) == 'null':
                            st.markdown(f'`{null}`')
                        else:
                            st.json(str(qry_result))
                    else:
                        st.write(qry_result)
    with exec_tab:
        with st.form('exec-form'):
            st.warning(
                '''The Exec Form does not accept any arguments and will not retrieve the
                   results of the query.''')
            qry_exec = st.text_area(
                'EdgeDB Execute',
                placeholder='Example: \nINSERT Movie {title := "John Wick 5"};')
            *_, qry_exec_last_col = st.columns(7)
            with qry_exec_last_col:
                qry_exec_submit_button = st.form_submit_button('Execute')
            if qry_exec_submit_button:
                conn.execute(qry_exec)
                st.toast('Query executed successfully', icon="✅")

    with benchmark_tab:
        with open('images/benchmarks.svg') as f:
            svg = f.read()
        svg_html = render_svg(svg)
        st.write(svg_html, unsafe_allow_html=True)


if __name__ == '__main__':
    main()
