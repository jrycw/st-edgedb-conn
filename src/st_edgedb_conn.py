from contextlib import AbstractContextManager

import edgedb
import streamlit as st
from edgedb import Client as EdgeDBClient
from edgedb import Object as EdgeDBObject
from edgedb.blocking_client import Iteration, Retry
from streamlit.connections import ExperimentalBaseConnection


class WrongQueryParamsError(Exception):
    pass


def match_func_name(jsonify=False, required_single=None):
    match (jsonify, required_single):
        case (False, None):
            func_name = 'query'
        case (False, False):
            func_name = 'query_single'
        case (False, True):
            func_name = 'query_required_single'
        case (True, None):
            func_name = 'query_json'
        case (True, False):
            func_name = 'query_single_json'
        case (True, True):
            func_name = 'query_required_single_json'
        case (_, _):
            raise WrongQueryParamsError(f'''{jsonify} must be True/False and 
                                        {required_single} must be True/False/None
                                        ''')
    return func_name


class EdgeDBConnection(ExperimentalBaseConnection[EdgeDBClient], AbstractContextManager):
    _ENV_EDGEDB_DSN = 'EDGEDB_DSN'

    def __init__(self, connection_name: str = "edgedb_conn", **kwargs) -> None:
        super().__init__(connection_name, **kwargs)

    @property
    def _dsn(self) -> str:
        '''
        Use the specified dsn in the code, if provided.
        If not specified, attempt to retrieve the dsn from st.secrets. 
        If not found, raise a KeyError.
        '''
        return self._kwargs.get('dsn') or self._secrets[self._ENV_EDGEDB_DSN]

    def _connect(self, **kwargs) -> EdgeDBClient:
        return edgedb.create_client(dsn=self._dsn)

    @property
    def client(self) -> EdgeDBClient:
        return self._instance

    def query(self,
              qry: str,
              *args,
              ttl: int | None = None,
              jsonify: bool = False,
              required_single: bool | None = None,
              **kwargs) -> str | EdgeDBObject:
        func_name = match_func_name(jsonify, required_single)
        mutated_kws = ('insert', 'update', 'delete')
        is_mutated = any(mutated_kw in qry.casefold()
                         for mutated_kw in mutated_kws)
        if is_mutated:
            ttl = 0

        @st.cache_resource(ttl=ttl, show_spinner='Executing your query...')
        def _query(func_name, qry, *args, **kwargs):
            # print(f'_query called, {ttl=}')  # For DEBUG
            return getattr(self.client, func_name)(qry, *args, **kwargs)

        return _query(func_name, qry, *args, **kwargs)

    def execute(self, qry) -> None:
        return self.client.execute(qry)

    def close(self) -> None:
        self.client.close()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def transaction(self) -> Retry:
        return self.client.transaction()

    def __call__(self) -> Iteration:
        for tx in self.transaction():
            with tx:
                yield tx

    @property
    def _status_alive_urls(self):
        """https://www.edgedb.com/docs/guides/deployment/health_checks#health-checks"""
        from yarl import URL

        url = URL(self._dsn)
        return [f'http{s}://{url.host}:{url.port}/server/status/alive'
                for s in ('', 's')]

    def is_healthy(self):
        import httpx
        return any(httpx.get(url,
                             follow_redirects=True,
                             verify=False,
                             timeout=30).status_code == 200
                   for url in self._status_alive_urls)
