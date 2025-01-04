import taos

class TDengineSetup:
    def __init__(self, host="localhost", user="root", password="taosdata", database="testdb"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        """Establish a connection to TDengine."""
        if self.conn is None:
            self.conn = taos.connect(host=self.host, user=self.user, password=self.password)
        return self.conn

    def create_database(self):
        """Create the database if it doesn't exist."""
        self.connect().execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.connect().execute(f"USE {self.database}")

    def create_super_table(self, table_name, schema, tags):
        """Create a super table if it doesn't exist."""
        create_stable_query = f"""
        CREATE STABLE IF NOT EXISTS {table_name} (
            {schema}
        ) TAGS ({tags})
        """
        self.connect().execute(create_stable_query)

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
