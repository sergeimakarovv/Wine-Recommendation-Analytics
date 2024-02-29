from typing import List
from flask import Flask, jsonify
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
import database





app = Flask(__name__)



def __get_connection():
    return psycopg2.connect(os.environ["DATABASE_URI"])


@app.route('/wineries', methods=['GET'])
def get_wineries() -> List:
    """
    Returns: Endpoint returns data about wineries
    """
    try:
        with app.app_context():
            connection = __get_connection()
            query = database.retrieve_sql_dict["wineries"]
            data = database.get_data(connection, table_name="wineries", query=query)
            print(data)
            return jsonify(data), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred"}), 500


@app.route('/wine', methods=['GET'])
def get_wine() -> List:
    """
    Returns: Endpoint returns data about wines
    """
    try:
        with app.app_context():
            connection = __get_connection()
            query = database.retrieve_sql_dict["wines"]
            data = database.get_data(connection, table_name="wines", query=query)
            print(data)
            return jsonify(data), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred"}), 500


@app.route('/ratings', methods=['GET'])
def get_ratings() -> List:
    """
    Returns: Endpoint returns data about ratings
    """
    try:
        with app.app_context():
            connection = __get_connection()
            query = database.retrieve_sql_dict["avg_ratings"]
            data = database.get_data(connection, table_name="avg_ratings", query=query)
            print(data)
            return jsonify(data), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred"}), 500


@app.route('/harmonizer', methods=['GET'])
def get_harmonizer_picture() -> List:
    """
    Returns: Endpoint returns data about harmonizers/food/snacks which are compliment wines
    """
    try:
        with app.app_context():
            connection = __get_connection()
            query = database.retrieve_sql_dict["harmonizer_picture"]
            data = database.get_data(connection, table_name="harmonizer_picture", query=query)
            print(data)
            return jsonify(data), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred"}), 500



if __name__ == "__main__":
   app.run(port=8080)

