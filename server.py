import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Optional

import dotenv
from flask import Flask, jsonify, request, g

app = Flask(__name__)

dotenv.load_dotenv()

# Check if the environment variables exist
if not os.environ.get('TREE_FILE'):

    tree_file = input("Enter the path to the tree file (default tree.db): ")

    if not tree_file:
        tree_file = os.path.join(os.path.dirname(__file__), 'tree.db')

    os.environ['TREE_FILE'] = tree_file

if not os.environ.get('TREE_JSON'):

    tree_json = input("Enter the path to the tree JSON file (leave blank if none): ")
    os.environ['TREE_JSON'] = tree_json

if not os.environ.get('TREE_ID'):

    tree_id = input("Enter the tree ID: ")
    os.environ['TREE_ID'] = tree_id

if not os.environ.get('SERVER_PASSWORD'):

    server_password = input("Enter the server password: ")
    os.environ['SERVER_PASSWORD'] = server_password

    # Hash the server password
    server_password_hash = hashlib.sha256(os.environ['SERVER_PASSWORD'].encode()).hexdigest()
    os.environ['SERVER_PASSWORD_HASH'] = server_password_hash

if not os.environ.get('SERVER_PORT'):

    server_port = input("Enter the server port (leave blank for default 8080): ")

    if not server_port:
        server_port = '8080'

    os.environ['SERVER_PORT'] = server_port

# Get the environment variables
TREE_FILE = os.environ['TREE_FILE']
TREE_ID = os.environ['TREE_ID']
TREE_JSON = os.environ['TREE_JSON']
SERVER_PORT = os.environ['SERVER_PORT']
SERVER_PASSWORD_HASH = os.environ['SERVER_PASSWORD_HASH']

def delete_existing_database():
    """
    If TREE_JSON is specified, delete the existing database.
    """
    if os.environ.get('TREE_JSON') and os.path.exists(os.environ['TREE_JSON']):
        if os.path.exists(os.environ['TREE_FILE']):
            os.remove(os.environ['TREE_FILE'])
    
def create_nodes_table(filename=TREE_FILE):
    """
    Create the nodes table.
    """
    db_conn = sqlite3.connect(filename)
    cursor = db_conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS nodes
                 (id TEXT PRIMARY KEY,
                  parent_ids TEXT,
                  children_ids TEXT,
                  text TEXT,
                  author TEXT,
                  timestamp TEXT)''')
    db_conn.commit()
    db_conn.close()

def create_history_table(filename=TREE_FILE):
    """
    Create the history table (just node ids, timestamps, and operations).
    """
    db_conn = sqlite3.connect(filename)
    cursor = db_conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS history
                 (id TEXT PRIMARY KEY,
                  timestamp TEXT,
                  operation TEXT,
                  author TEXT)''')
    db_conn.commit()
    db_conn.close()

def load_tree_json_to_database(filename=TREE_JSON):
    """
    If TREE_JSON exists, load it into the database.
    """
    if os.environ.get('TREE_JSON') and os.path.exists(os.environ['TREE_JSON']):
        with open(filename, encoding='utf-8') as json_file:
            tree_json_data = json.load(json_file)

            # get current timestamp
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            db_conn = sqlite3.connect(TREE_FILE)
            cursor = db_conn.cursor()
            for node in tree_json_data['nodes']:
                node_data = tree_json_data['nodes'][node]
                # get parent ID(s)
                if 'parentIds' in node_data:
                    parent_ids = ','.join(node_data['parentIds'])
                else:
                    parent_ids = node_data['parentId']
                # get children ID(s)
                if 'childrenIds' in node_data:
                    children_ids = ','.join(node_data['childrenIds'])
                else:
                    # we have to find the children IDs from the tree_json_data
                    children_ids = []
                    for child in tree_json_data['nodes']:
                        if 'parentIds' in tree_json_data['nodes'][child]:
                            if node in tree_json_data['nodes'][child]['parentIds']:
                                children_ids.append(child)
                    children_ids = ','.join(children_ids)
                # insert the node into the database
                cursor.execute("INSERT INTO nodes (id, parent_ids, children_ids, text, author, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                            (node, parent_ids, children_ids, node_data['text'], "Morpheus", timestamp))
            db_conn.commit()
            db_conn.close()

def is_authorized(key):
    """
    Check if a user is authorized to make changes to the database.

    Args:
        key (str): The key to check.

    Returns:
        bool: True if the key is valid, False otherwise.
    """
    test = hashlib.sha256(key.encode()).hexdigest() == SERVER_PASSWORD_HASH
    return bool(test)

def get_db():
    """
    Creates a new connection and cursor object for the current thread if one does not exist.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = sqlite3.connect('tree.db')
    return g.sqlite_db, g.sqlite_db.cursor()

@app.teardown_appcontext
def close_db(error):
    """
    Closes the database connection at the end of the request.
    """
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


@app.route('/nodes', methods=['POST'])
# Define a route for saving a set of new nodes to the database
@app.route('/nodes/batch', methods=['POST'])
def save_nodes():
    """
    Saves a set of new nodes to the database.

    Returns:
        A JSON object containing a success flag.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    db_conn, cursor = get_db()
    data = request.get_json()
    for node in data:
        # get parent id(s)
        if 'parentIds' in node:
            parent_ids = ','.join(node['parentIds'])
        else:
            parent_ids = node['parentId']
        # get children ID(s)
        if 'childrenIds' in node:
            children_ids = ','.join(node['childrenIds'])
        else:
            children_ids = ''
        text = node['text']
        author = node['author']
        timestamp = node['timestamp']
        if 'id' in node:
            node_id = node['id']
        else:
            # generate a new ID for the node
            node_id = uuid.uuid4().hex
        cursor.execute("INSERT INTO nodes (id, parent_ids, children_ids, text, author, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                            (node_id, parent_ids, children_ids, text, author, timestamp))
        # Add the operation to the history table
        cursor.execute("INSERT INTO history (timestamp, id, operation, author) VALUES (?, ?, ?, ?)",
                            (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), node_id, 'create', author))
    db_conn.commit()
    return jsonify({'success': True})

@app.route('/nodes/<node_id>', methods=['PUT'])
def update_node(node_id):
    """
    Update an existing node in the database
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    db_conn, cursor = get_db()
    data = request.get_json()
    text = data['text']
    author = data['author']
    node_timestamp = data['timestamp']
    cursor.execute("UPDATE nodes SET text = ?, author = ?, timestamp = ? WHERE id = ?",
                  (text, author, node_timestamp, node_id))
    # Add the operation to the history table
    cursor.execute("INSERT INTO history (timestamp, id, operation, author) VALUES (?, ?, ?, ?)",
                (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), node_id, 'update', author))
    db_conn.commit()
    return jsonify({'success': True})

@app.route('/nodes/batch', methods=['PUT'])
def update_nodes():
    """
    Update a set of existing nodes in the database
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    db_conn, cursor = get_db()
    data = request.get_json()
    for node in data:
        node_id = node['id']
        text = node['text']
        author = node['author']
        node_timestamp = node['timestamp']
        cursor.execute("UPDATE nodes SET text = ?, author = ?, timestamp = ? WHERE id = ?",
                  (text, author, node_timestamp, node_id))
        # Add the operation to the history table
        cursor.execute("INSERT INTO history (timestamp, id, operation, author) VALUES (?, ?, ?, ?)",
                    (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), node_id, 'update', author))
    db_conn.commit()
    db_conn.close()
    return jsonify({'success': True})

@app.route('/nodes/<node_id>', methods=['DELETE'])
def delete_node(node_id):
    """
    Delete a node from the database.

    Args:
        node_id (str): The ID of the node to delete.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the deletion was successful.

    Raises:
        KeyError: If the node ID is not found in the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    db_conn, cursor = get_db()
    cursor.execute("DELETE FROM nodes WHERE id = ?", (node_id,))
    # Add the operation to the history table
    author = request.args.get('author')
    cursor.execute("INSERT INTO history (timestamp, id, operation, author) VALUES (?, ?, ?, ?)",
                (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), node_id, 'delete', author))
    db_conn.commit()
    return jsonify({'success': True})

@app.route('/nodes/batch', methods=['DELETE'])
def delete_nodes():
    """
    Delete a set of nodes from the database.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the deletion was successful.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    db_conn, cursor = get_db()
    data = request.get_json()
    for node_data in data:
        node_id = node_data['id']
        cursor.execute("DELETE FROM nodes WHERE id = ?", (node_id,))
        # Add the operation to the history table
        author = request.args.get('author')
        cursor.execute("INSERT INTO history (timestamp, id, operation, author) VALUES (?, ?, ?, ?)",
                    (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), node_id, 'delete', author))
    db_conn.commit()
    return jsonify({'success': True})

@app.route('/nodes/exists/<node_id>', methods=['GET'])
def node_exists(node_id):
    """
    Check if a node exists in the database.

    Args:
        node_id (str): The ID of the node to check.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the check was successful, and an 'exists' key with a boolean value indicating whether the node exists in the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})

    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})

    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
    exists = cursor.fetchone() is not None

    return jsonify({'success': True, 'exists': exists})
    
@app.route('/nodes/exists', methods=['POST'])
def nodes_exist():
    """
    Check if a list of nodes exists in the database.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the check was successful, and an 'exists' key with a dictionary of node IDs and their existence status in the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})

    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})

    cursor = get_db()[1]
    data = request.get_json()
    node_ids = data['nodeIds']
    exists = {}
    for node_id in node_ids:
        cursor.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
        node = cursor.fetchone()
        if node:
            exists[node_id] = True
        else:
            exists[node_id] = False
    return jsonify({'success': True, 'exists': exists})

@app.route('/nodes/get/<timestamp>', methods=['GET'])
def get_nodes(timestamp):
    """
    Define a route for getting all nodes from the database after a given timestamp.

    Args:
        timestamp (str): The timestamp to use as a filter for the nodes.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the operation was successful, and a 'nodes' key with a list of nodes retrieved from the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE timestamp > ?", (timestamp.replace("%"," "),))
    nodes = cursor.fetchall()
    # jsonify the nodes
    nodes = [{
        'id': node[0],
        'parent_ids': node[1].split(',') if node[1] else None,
        'children_ids': node[2].split(',') if node[2] else None,
        'text': node[3],
        'author': node[4],
        'timestamp': node[5]
    } for node in nodes]
    return jsonify({'success': True, 'nodes': nodes})

# Define a route for getting all node ids from the database
@app.route('/nodes/ids', methods=['GET'])
def get_all_node_ids():
    """
    Define a route for getting all node ids from the database.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the operation was successful, and a 'nodes' key with a list of node ids retrieved from the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT id FROM nodes")
    nodes = cursor.fetchall()
    # jsonify the nodes
    nodes = [node[0] for node in nodes]
    return jsonify({'success': True, 'nodes': nodes})

@app.route('/nodes', methods=['GET'])
def get_all_nodes():
    """
    Define a route for getting all nodes from the database.

    Returns:
        dict: A dictionary containing a 'success' key with a boolean value indicating whether the operation was successful, and a 'nodes' key with a dictionary of nodes retrieved from the database.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes")
    nodes = cursor.fetchall()
    # jsonify the nodes
    nodes = {node[0]:{
        'parent_ids': node[1].split(',') if node[1] else None,
        'children_ids': node[2].split(',') if node[2] else None,
        'text': node[3],
        'author': node[4],
        'timestamp': node[5]
    } for node in nodes}
    return jsonify({'success': True, 'nodes': nodes})

@app.route('/nodes/count', methods=['GET'])
def get_node_count():
    """
    Get the number of nodes in the database.

    Returns:
        A dictionary containing the success status and the count of nodes.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT COUNT(*) FROM nodes")
    count = cursor.fetchone()[0]
    return jsonify({'success': True, 'count': count})

@app.route('/nodes/<node_id>', methods=['GET'])
def get_node(node_id):
    """
    Get a single node from the database.

    Args:
        node_id: The id of the node to retrieve.

    Returns:
        A dictionary containing the success status and the retrieved node.
    """

    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
    retrieved_node = cursor.fetchone()
    # jsonify the node
    node = {
        'id': retrieved_node[0],
        'parent_ids': retrieved_node[1].split(',') if retrieved_node[1] else None,
        'children_ids': retrieved_node[2].split(',') if retrieved_node[2] else None,
        'text': retrieved_node[3],
        'author': retrieved_node[4],
        'timestamp': retrieved_node[5]
    }
    return jsonify({'success': True, 'node': node})
@app.route('/nodes/root', methods=['GET'])
def get_root_node():
    """
    Get the root node from the database.

    Returns:
        A dictionary containing the success status and the root node.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE parent_ids IS NULL")
    root_node = cursor.fetchone()
    # jsonify the node
    root_node = {
        'id': root_node[0],
        'parent_ids': root_node[1].split(',') if root_node[1] else None,
        'children_ids': root_node[2].split(',') if root_node[2] else None,
        'text': root_node[3],
        'author': root_node[4],
        'timestamp': root_node[5]
    }
    return jsonify({'success': True, 'node': root_node})

@app.route('/nodes/<node_id>/children', methods=['GET'])
def get_children(node_id):
    """
    Get the children of a node from the database.

    Args:
        node_id: The ID of the node whose children to retrieve.

    Returns:
        A dictionary containing the success status and the child nodes.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE parent_ids LIKE ?", ('%'+node_id+'%',))
    nodes = cursor.fetchall()
    # jsonify the nodes
    nodes = [{
        'id': node[0],
        'parent_ids': node[1].split(',') if node[1] else None,
        'children_ids': node[2].split(',') if node[2] else None,
        'text': node[3],
        'author': node[4],
        'timestamp': node[5]
    } for node in nodes]
    return jsonify({'success': True, 'nodes': nodes})

@app.route('/nodes/<node_id>/parents', methods=['GET'])
def get_parents(node_id):
    """
    Get the parents of a node from the database.

    Args:
        node_id: The ID of the node whose parents to retrieve.

    Returns:
        A dictionary containing the success status and the parent nodes.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM nodes WHERE children_ids LIKE ?", ('%'+node_id+'%',))
    nodes = cursor.fetchall()
    # jsonify the nodes
    nodes = [{
        'id': node[0],
        'parent_ids': node[1].split(',') if node[1] else None,
        'children_ids': node[2].split(',') if node[2] else None,
        'text': node[3],
        'author': node[4],
        'timestamp': node[5]
    } for node in nodes]
    return jsonify({'success': True, 'nodes': nodes})


@app.route('/history', methods=['GET'])
def get_history():
    """
    Get history from the database.

    Returns:
        A dictionary containing the success status and the history.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM history")
    history = cursor.fetchall()
    # jsonify the history
    history = [{
        'node_id': h[0],
        'timestamp': h[1],
        'operation': h[2],
        'author': h[3]
    } for h in history]
    return jsonify({'success': True, 'history': history})

@app.route('/history/<timestamp>', methods=['GET'])
def get_history_after(timestamp: str):
    """
    Get history from the database after a certain timestamp.

    Args:
        timestamp (str): The timestamp to get the history after.

    Returns:
        A dictionary containing the success status and the history after the given timestamp.
    """
    # Check if the user is authorized to make changes to the database
    if not is_authorized(request.headers.get('Authorization')):
        return jsonify({'success': False, 'error': 'Unauthorized'})
    # Check if the tree id is correct
    if request.headers.get('Tree-Id') != TREE_ID:
        return jsonify({'success': False, 'error': 'Invalid Tree-Id'})
    cursor = get_db()[1]
    cursor.execute("SELECT * FROM history WHERE timestamp > ?", (timestamp,))
    history = cursor.fetchall()
    # jsonify the history
    history = [{
        'node_id': h[0],
        'timestamp': h[1],
        'operation': h[2],
        'author': h[3]
    } for h in history]
    return jsonify({'success': True, 'history': history})

def init_db():
    delete_existing_database()
    create_nodes_table()
    create_history_table()
    load_tree_json_to_database()

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='127.0.0.1', port=int(SERVER_PORT), threaded=True)