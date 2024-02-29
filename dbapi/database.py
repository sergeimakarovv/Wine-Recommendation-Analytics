from typing import Dict, List
import psycopg2
import psycopg2.extras



table_create_sql_dict = {

    "wineries": """ CREATE TABLE IF NOT EXISTS Wineries (
    WineryID INTEGER PRIMARY KEY,
    WineryName VARCHAR(255),
    Website VARCHAR(90)
); """,

    "wines": """ CREATE TABLE IF NOT EXISTS Wines (
    WineID INTEGER PRIMARY KEY,
    WineName VARCHAR(69),
    Type VARCHAR(12),
    Elaborate VARCHAR(33),
    Grapes TEXT,
    ABV FLOAT,
    Body VARCHAR(17),
    Acidity VARCHAR(6),
    Vintages TEXT,
    Harmonize TEXT,
    WineryID INTEGER,
    Code VARCHAR(2),
    Country VARCHAR(14),
    RegionID INTEGER,
    RegionName VARCHAR(68),
    URL VARCHAR(31),
    FOREIGN KEY (WineryID) REFERENCES Wineries(WineryID)
); """,
    "ratings": """ CREATE TABLE IF NOT EXISTS Ratings (
    RatingID INTEGER PRIMARY KEY,
    UserID INTEGER,
    WineID INTEGER,
    Vintage VARCHAR(255),
    Rating FLOAT,
    Date TIMESTAMP,
    FOREIGN KEY (WineID) REFERENCES Wines(WineID)
); """,

    "harmonizer_picture": """ CREATE TABLE IF NOT EXISTS HarmonizerPicture (
    Snacks VARCHAR(16) PRIMARY KEY,
    URL VARCHAR(31)
); """
}

inser_sql_dict = {

    "wines": """ INSERT INTO Wines 
    (WineID, WineName,Type,Elaborate,Grapes,ABV,Body,Acidity,Vintages,Harmonize,WineryID,Code ,Country ,RegionID ,RegionName ,URL ) VALUES 
    (%s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
""",

    "ratings": """ 
    INSERT INTO Ratings (RatingID, UserID, WineID, Vintage, Rating, Date) VALUES 
    (%s, %s, %s, %s, %s, %s);

""",

    "wineries": """ 
    INSERT INTO Wineries (WineryID, WineryName, Website) VALUES 
    (%s, %s, %s);


""",

    "harmonizer_picture": """ INSERT INTO HarmonizerPicture (Snacks, URL) VALUES 
                              (%s, %s);
"""

}

retrieve_sql_dict = {

    "wines": """SELECT * FROM Wines;""",

    "ratings": """SELECT * FROM Ratings LIMIT 2000;""",

    "wineries": """SELECT * FROM Wineries;""",

    "harmonizer_picture": """SELECT * FROM HarmonizerPicture;""",

    "avg_ratings":  """ 
                SELECT ROUND(AVG(rating)::numeric, 2)::float AS avg_rating, wineid 
                FROM Ratings
                GROUP BY wineid;

"""

}

GET_TABLE_NAMES = """
    SELECT tablename FROM pg_catalog.pg_tables 
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
    """

# url = ""

# connection = psycopg2.connect(url)


# Creating tables functions: wines, winery, rating, snaks


def create_db(connection) -> None:
    """
    Creates database.

    Parameters:
    connection: A psycopg2 connection object for database operations.
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE WineData;")


def create_table(connection, tables_dict: dict) -> None:
    """
    Creates database tables from a dictionary of SQL queries.

    Parameters:
        connection: A psycopg2 connection object for database operations.
        tables_dict: A dictionary mapping table names to SQL 'CREATE TABLE' query strings.

    Example:
        tables_sql_dict = {
            'table_name': 'CREATE TABLE table_name (column, column2, column3....)',
            'table_name2': 'CREATE TABLE table_name (column, column2, column3....)
            }
        create_tables(db_connection, tables_sql_dict)
    """
    for table_name in tables_dict.keys():
        print(table_name)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(tables_dict[table_name])


# Inserting data functions: wines, winery, rating, snacks
def inser_winery_data(connection, data: tuple) -> None:
    """
    Inserts a single row of data into the 'wineries' table.

    Parameters:
        connection: A psycopg2 connection object for database operations.
        data: A tuple representing a single row of data to insert into the 'wineries' table.

    Note:
        The 'data' tuple should match the column order and data types expected by the 'wineries' table.
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(inser_sql_dict["wineries"], data)


def inser_wines_data(connection, data: tuple) -> None:
    """
    Inserts a single row of data into the 'wines' table.

    Parameters:
        connection: A psycopg2 connection object for database operations.
        data: A tuple representing a single row of data to insert into the 'wines' table.

    Note:
        The 'data' tuple should match the column order and data types expected by the 'wines' table.
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(inser_sql_dict["wines"], data)


def inser_rating_data(connection, data: tuple) -> None:
    """
    Inserts a single row of data into the 'ratings' table.

    Parameters:
        connection: A psycopg2 connection object for database operations.
        data: A tuple representing a single row of data to insert into the 'ratings' table.

    Note:
        The 'data' tuple should match the column order and data types expected by the 'ratings' table.
    """
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(inser_sql_dict["ratings"], data)


def inser_snaks_data(connection, data: tuple) -> None:
    """
    Inserts a single row of data into the 'HarmonizerPicture' table.

    Parameters:
        connection: A psycopg2 connection object for database operations.
        data: A tuple representing a single row of data to insert into the 'HarmonizerPicture' table.

    Note:
        The 'data' tuple should match the column order and data types expected by the 'HarmonizerPicture' table.
    """

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(inser_sql_dict["harmonizer_picture"], data)


def bulk_inserting(connection, csv_file_name: str, table_name: str):
    """
    Inserts a bulk of data into a specified table from a CSV file.

    Parameters:
        connection: A psycopg2 connection object used to connect to the database.
        csv_file_name: The name of the CSV file containing data to be inserted. The file should be formatted with columns matching the target table's schema.
        table_name: The name of the table into which the data will be inserted.

    Note:
        Ensure the CSV file's columns match the column order and data types of the specified table. The example note refers to the 'HarmonizerPicture' table, so adapt it accordingly for different tables.
    """

    with connection:
        with connection.cursor() as cursor:
            with open(csv_file_name, 'r') as file:
                next(file)  # Skip the header row.
                cursor.copy_from(file, table_name, sep=',')


# Getting data function
def get_data(connection, table_name, query) -> List:
    """
        Retrieves data from a specified table and returns it as a list of dictionaries.

        Parameters:
            connection: A psycopg2 connection object used for database operations.
            table_name: The name of the table from which data is to be retrieved.
            query: The SQL query used to extract data from the specified table. The query should include the table name and any conditions or formatting required.

        Returns:
            List[dict]: A list where each element is a dictionary representing a row of data from the table, with keys as column names.

        Example:
            # Example SQL query
            query = "SELECT * FROM my_table WHERE condition = value;"
            data = get_data(db_connection, 'my_table', query)
            # data is now a list of dictionaries
    """
    with connection:
        print(query)
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return [dict(row) for row in result]


# Get name of the data
def list_tables(connection) -> List:
    """
    Retrieves a list of table names present in the connected database.

    Parameters:
        connection: A psycopg2 connection object used for database operations.

    Returns:
        List[str]: A list containing the names of all tables in the database.
"""
    with connection:
        with connection.cursor() as cursor:
            list_of_tables = cursor.execute(GET_TABLE_NAMES)
            print([table[0] for table in cursor.fetchall()])


