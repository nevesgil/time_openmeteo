import taosws


class TDengineSetup:
    def __init__(self, host="localhost", user="root", password="taosdata"):
        self.host = host
        self.user = user
        self.password = password
        self.conn = None
        self.database = None

    def connect(self):
        """Establish a connection to TDengine."""
        if self.conn is None:
            connection_url = f"taosws://{self.user}:{self.password}@{self.host}:6041"
            self.conn = taosws.connect(connection_url)
        return self.conn

    def set_database(self, database_name, **options):
        """Set the database name and create it if it doesn't exist."""
        self.database = database_name
        options_string = " ".join(
            [f"{key.upper()} {value}" for key, value in options.items()]
        )
        self.connect().execute(
            f"CREATE DATABASE IF NOT EXISTS {database_name} {options_string}"
        )
        self.connect().execute(f"USE {database_name}")

    def create_super_table(self, table_name, schema, tags):
        """Create a super table if it doesn't exist."""
        create_stable_query = f"""
        CREATE STABLE IF NOT EXISTS {table_name} (
            {schema}
        ) TAGS ({tags})
        """
        self.connect().execute(create_stable_query)

    def create_subtable(self, subtable_name, tags, values):
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {subtable_name} USING weather_data TAGS ({tags})
        """
        self.connect().execute(create_query)

        insert_query = f"""
        INSERT INTO {subtable_name} VALUES ({values})
        """
        self.connect().execute(insert_query)

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
