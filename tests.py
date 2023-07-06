"""
This module contains unit tests for the Multiloom server.
"""

import os
import sqlite3
import tempfile
import unittest

from flask import Flask, url_for
from hypothesis import example, given
from hypothesis import strategies as st

class TestServer(unittest.TestCase):
    """
    This class contains unit tests for the Multiloom server.
    """
    def create_test_db(self):
        """
        Create a temporary test database and return a connection to it
        """
        _, db_path = tempfile.mkstemp()
        conn = sqlite3.connect(db_path)
        tree_script = os.path.join(os.path.dirname(__file__), 'tree.sql')
        with open(tree_script, encoding='utf-8') as file:
            conn.executescript(file.read())
        return conn


    def setUp(self):
        """
        Set up the Flask test client and database before each test
        """
        self.app = Flask(__name__)
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
        self.db_conn = self.create_test_db()
        self.app.config['DATABASE'] = self.db_conn            

    def assert_response(self, response):
        """
        Assert that the response is valid and contains the expected data.

        Args:
            response (flask.Response): The response to check.

        Raises:
            AssertionError: If the response is not valid or does not contain the expected data.
        """
        response_json = response.json if response.content_type == 'application/json' else None

        if response_json is not None:
            print(response_json)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response_json['success'], True)
        else:
            print(response.data)
            self.fail('Response content type is not application/json')

    def tearDown(self):
        """
        Delete all nodes from the database after tests are finished
        """
        response = self.client.delete(url_for('nodes'), headers={
                                    "Authorization":"123456",
                                    "Tree-Id":"1"
                                })

        self.assert_response(response)
        self.db_conn.close()
        os.unlink(self.app.config['DATABASE'].name)

    @given(st.text(), st.text(), st.text(), st.text())
    @example('1', 'Test node', 'Test author', '2022-01-01 00:00:00')
    def test_save_node(self, parent_id, text, author, timestamp):
        """
        Test saving a new node to the database
        """
        data = {
            'parentId': parent_id,
            'text': text,
            'author': author,
            'timestamp': timestamp
        }
        response = self.client.post(url_for('nodes'), json=data,
                                    headers={
                                        "Authorization": "123456",
                                        "Tree-Id": "1"
                                    })
        self.assert_response(response)
        response_json = response.json if response.content_type == 'application/json' else None

        # Check that the node was actually saved to the database
        if response_json is not None:
            node = self.client.get(url_for('node', node_id=response_json['id']),
                                   headers={
                                       "Authorization": "123456",
                                       "Tree-Id": "1"
                                   })
            self.assert_response(node)

            self.assertIsNotNone(node.json)
            self.assertEqual(response_json['parentId'], parent_id)
            self.assertEqual(response_json['author'], author)
            self.assertEqual(str(response_json['timestamp']), timestamp)

    @given(st.text(), st.text(), st.text())
    @example('Updated node', 'Updated author', '2022-01-01 00:00:00')
    def test_update_node(self, text, author, timestamp):
        """
        Test updating an existing node in the database
        """
        data = {
            'text': text,
            'author': author,
            'timestamp': timestamp
        }
        response = self.client.put(url_for('node', node_id=1), json=data,
                                headers={
                                    "Authorization":"123456",
                                    "Tree-Id":"1"
                                })
        self.assert_response(response)

    @given(st.text())
    @example('2021-01-01 00:00:00')
    def test_get_nodes(self, timestamp):
        """
        Test getting all nodes from the database after a given timestamp
        """
        response = self.client.get(
            url_for('nodes_after_timestamp', timestamp=timestamp),
            headers={
                "Authorization": "123456",
                "Tree-Id": "1"
            }
        )
        self.assert_response(response)

    def test_get_node_ids(self):
        """
        Test getting all node ids from the database
        """
        response = self.client.get(url_for('node_ids'), 
                                headers={
                                    "Authorization":"123456",
                                    "Tree-Id":"1"
                                })
        self.assert_response(response)

    def test_node_exists(self):
        """
        Test checking if a node exists in the database
        """
        response = self.client.get(url_for('node_exists', node_id=1), headers={
                                    "Authorization":"123456",
                                    "Tree-Id":"1"
                                })
        self.assert_response(response)

    def test_get_history(self):
        """
        Test getting the history from the database
        """
        response = self.client.get(url_for('history'), headers={
                                    "Authorization":"123456",
                                    "Tree-Id":"1"
                                })
        self.assert_response(response)

if __name__ == '__main__':
    unittest.main()
