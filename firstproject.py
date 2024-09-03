import sqlite3

def create_connection(db_file):
    """ Create a database connection to the SQLite database specified by db_file """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """ Create a table from the create_table_sql statement """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)

def clear_table(conn, table_name):
    """ Clear all data from a table """
    try:
        c = conn.cursor()
        c.execute(f'DELETE FROM {table_name}')
        conn.commit()
    except sqlite3.Error as e:
        print(e)

def insert_data(conn, insert_sql, data):
    """ Insert data into table """
    try:
        c = conn.cursor()
        c.executemany(insert_sql, data)
        conn.commit()
    except sqlite3.Error as e:
        print(e)

def fetch_data(conn, table_name):
    """ Fetch all data from a table """
    try:
        c = conn.cursor()
        c.execute(f'SELECT * FROM {table_name}')
        rows = c.fetchall()
        return rows
    except sqlite3.Error as e:
        print(e)
        return []

def print_table_data(table_name, data):
    """ Print data from a table """
    print(f"\n{table_name} Table:")
    for row in data:
        print(row)

def main():
    database = "sports_data.db"

    sql_create_teams_table = """
    CREATE TABLE IF NOT EXISTS Teams (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        city TEXT
    );
    """

    sql_create_players_table = """
    CREATE TABLE IF NOT EXISTS Players (
        player_id INTEGER PRIMARY KEY,
        player_name TEXT,
        team_id INTEGER,
        position TEXT,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id)
    );
    """

    sql_create_matches_table = """
    CREATE TABLE IF NOT EXISTS Matches (
        match_id INTEGER PRIMARY KEY,
        date TEXT,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        FOREIGN KEY (home_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY (away_team_id) REFERENCES Teams(team_id)
    );
    """

    sql_create_statistics_table = """
    CREATE TABLE IF NOT EXISTS Statistics (
        stat_id INTEGER PRIMARY KEY,
        match_id INTEGER,
        player_id INTEGER,
        points INTEGER,
        assists INTEGER,
        rebounds INTEGER,
        FOREIGN KEY (match_id) REFERENCES Matches(match_id),
        FOREIGN KEY (player_id) REFERENCES Players(player_id)
    );
    """

    # Create a database connection
    conn = create_connection(database)

    # Create tables
    if conn is not None:
        create_table(conn, sql_create_teams_table)
        create_table(conn, sql_create_players_table)
        create_table(conn, sql_create_matches_table)
        create_table(conn, sql_create_statistics_table)
    else:
        print("Error! Cannot create the database connection.")

    # Clear existing data
    clear_table(conn, 'Teams')
    clear_table(conn, 'Players')
    clear_table(conn, 'Matches')
    clear_table(conn, 'Statistics')

    # Insert sample data into Teams
    teams_data = [
        (1, 'Lions', 'New York'),
        (2, 'Tigers', 'Los Angeles'),
        (3, 'Bears', 'Chicago')
    ]
    insert_data(conn, 'INSERT INTO Teams (team_id, team_name, city) VALUES (?, ?, ?)', teams_data)

    # Insert sample data into Players
    players_data = [
        (1, 'John Doe', 1, 'Forward'),
        (2, 'Jane Smith', 2, 'Guard'),
        (3, 'Mike Brown', 3, 'Center')
    ]
    insert_data(conn, 'INSERT INTO Players (player_id, player_name, team_id, position) VALUES (?, ?, ?, ?)', players_data)

    # Insert sample data into Matches
    matches_data = [
        (1, '2023-01-01', 1, 2, 100, 98),
        (2, '2023-01-02', 2, 3, 95, 102)
    ]
    insert_data(conn, 'INSERT INTO Matches (match_id, date, home_team_id, away_team_id, home_score, away_score) VALUES (?, ?, ?, ?, ?, ?)', matches_data)

    # Insert sample data into Statistics
    statistics_data = [
        (1, 1, 1, 30, 5, 10),
        (2, 1, 2, 25, 7, 8),
        (3, 2, 3, 20, 3, 12)
    ]
    insert_data(conn, 'INSERT INTO Statistics (stat_id, match_id, player_id, points, assists, rebounds) VALUES (?, ?, ?, ?, ?, ?)', statistics_data)

    # Fetch and print data from each table
    tables = ['Teams', 'Players', 'Matches', 'Statistics']
    for table in tables:
        data = fetch_data(conn, table)
        print_table_data(table, data)
    
    # Close the connection
    conn.close()

if __name__ == '__main__':
    main()