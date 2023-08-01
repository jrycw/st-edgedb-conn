import unittest

import edgedb

from src.st_edgedb_conn import (
    EdgeDBConnection,
    WrongQueryParamsError,
    match_func_name,
)


class TestUtils(unittest.TestCase):
    def test_match_func_name(self):
        query = match_func_name()
        self.assertEqual('query', query)

        query_single = match_func_name(required_single=False)
        self.assertEqual('query_single', query_single)

        query_required_single = match_func_name(required_single=True)
        self.assertEqual('query_required_single', query_required_single)

        query_json = match_func_name(jsonify=True)
        self.assertEqual('query_json', query_json)

        query_single_json = match_func_name(
            jsonify=True, required_single=False)
        self.assertEqual('query_single_json', query_single_json)

        query_required_single_json = match_func_name(
            jsonify=True, required_single=True)
        self.assertEqual('query_required_single_json',
                         query_required_single_json)

        with self.assertRaises(WrongQueryParamsError):
            match_func_name(jsonify='123', required_single=0)


class TestConn(unittest.TestCase):
    """ The test relies on true EdgeDB instance and its built-in `_example` database.
        No mocking is adopted."""

    @classmethod
    def setUpClass(cls):
        cls.conn = EdgeDBConnection()
        cls.client = edgedb.create_client(dsn=cls.conn._dsn)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.client.close()

    def test_query(self):
        qry = '''SELECT Movie {title};'''
        r1 = self.conn.query(qry)
        r2 = self.client.query(qry)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_one_item_with_positional_args(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$0 LIMIT 1;'''
        title = 'The Avengers'
        r1 = self.conn.query(qry, title, required_single=False)
        r2 = self.client.query_single(qry, title)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_one_item_with_named_args(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$title LIMIT 1;'''
        title = 'The Avengers'
        r1 = self.conn.query(qry, title=title, required_single=False)
        r2 = self.client.query_single(qry, title=title)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_no_item(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$title LIMIT 1;'''
        title = 'The Avengers_123456789abcdefg'
        r1 = self.conn.query(qry, title=title, required_single=False)
        r2 = self.client.query_single(qry, title=title)
        self.assertIsNone(r1)
        self.assertIsNone(r2)

    def test_query_required_single_one_item_with_positional_args(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$0 LIMIT 1;'''
        title = 'Spider-Man: Homecoming'
        r1 = self.conn.query(qry, title, required_single=True)
        r2 = self.client.query_required_single(qry, title)
        self.assertEqual(str(r1), str(r2))

    def test_query_required_single_one_item_with_named_args(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$title LIMIT 1;'''
        title = 'Spider-Man: Homecoming'
        r1 = self.conn.query(qry, title=title, required_single=True)
        r2 = self.client.query_required_single(qry, title=title)
        self.assertEqual(str(r1), str(r2))

    def test_query_required_single_no_item(self):
        qry = '''SELECT Movie {title} FILTER .title = <str>$title LIMIT 1;'''
        title = 'Spider-Man: Homecoming_123456789abcdefg'
        with self.assertRaises(edgedb.errors.NoDataError):
            self.conn.query(qry, title=title, required_single=True)
        with self.assertRaises(edgedb.errors.NoDataError):
            self.client.query_required_single(qry, title=title)

    def test_query_json(self):
        qry = '''SELECT Person {name};'''
        r1 = self.conn.query(qry, jsonify=True)
        r2 = self.client.query_json(qry)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_json_one_item_with_positional_args(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$0));'''
        name = 'Cate Shortland'
        r1 = self.conn.query(qry, name, jsonify=True, required_single=False)
        r2 = self.client.query_single_json(qry, name)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_json_one_item_with_named_args(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$name));'''
        name = 'Cate Shortland'
        r1 = self.conn.query(
            qry, name=name, jsonify=True, required_single=False)
        r2 = self.client.query_single_json(qry, name=name)
        self.assertEqual(str(r1), str(r2))

    def test_query_single_json_no_item(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$name));'''
        name = 'Cate Shortland_123456789abcdefg'
        r1 = self.conn.query(
            qry, name=name, jsonify=True, required_single=False)
        r2 = self.client.query_single_json(qry, name=name)
        self.assertEqual(r1, 'null')
        self.assertEqual(r2, 'null')

    def test_query_required_single_json_one_item_with_positional_args(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$0));'''
        name = 'Rhys Ifans'
        r1 = self.conn.query(qry, name, jsonify=True, required_single=True)
        r2 = self.client.query_required_single_json(qry, name)
        self.assertEqual(str(r1), str(r2))

    def test_query_required_single_json_one_item_with_named_args(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$name));'''
        name = 'Rhys Ifans'
        r1 = self.conn.query(
            qry, name=name, jsonify=True, required_single=True)
        r2 = self.client.query_required_single_json(qry, name=name)
        self.assertEqual(str(r1), str(r2))

    def test_query_required_single_json_no_item(self):
        qry = '''SELECT assert_single((SELECT Person {name} FILTER .name = <str>$name));'''
        name = 'Rhys Ifans_123456789abcdefg'
        with self.assertRaises(edgedb.errors.NoDataError):
            self.conn.query(
                qry, name=name, jsonify=True, required_single=True)
        with self.assertRaises(edgedb.errors.NoDataError):
            self.client.query_required_single_json(qry, name=name)

    def test_execute(self):
        qry = '''SELECT Account {username};'''
        self.conn.execute(qry)
        self.client.execute(qry)

    def test_transation(self):
        qry = '''SELECT Show {title};'''
        for tx1 in self.client.transaction():
            with tx1:
                r1 = tx1.query(qry)
        for tx2 in self.conn():
            r2 = tx2.query(qry)
        self.assertEqual(str(r1), str(r2))


if __name__ == '__main__':
    unittest.main()
