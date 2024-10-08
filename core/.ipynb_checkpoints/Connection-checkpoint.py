import psycopg2
from psycopg2 import sql
import pandas as pd
class Connection:
    def __init__(self,id,host,port,username,password,database):
        self.id = id
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.session = None
        self.conn=None
        self.database=database
    def Session(self):
        db_config = {
            'user': self.username,
            'password': self.password,
            'host': self.host,  # Example: 'localhost' or an IP address
            'port': self.port,   # Example: '5432' is the default PostgreSQL port
            'database':self.database
        }
        self.conn = psycopg2.connect(**db_config)
        self.session = self.conn.cursor()
    def close(self):
        if self.session:
            self.session.close()
        if self.conn:
            self.conn.close()
    def query(self,code):
        if not self.session:
            self.Session()
        self.session.execute(sql.SQL(code))
        result = self.session.fetchall()
        return result
    def query_to_df(self,code):
        if not self.session:
            self.Session()
        self.session.execute(sql.SQL(code))
        result = self.session.fetchall()
        col_names = [desc[0] for desc in self.session.description]

        # Create a DataFrame from the result
        df = pd.DataFrame(result, columns=col_names)
        return df
    def df_to_table(self, df, table,database,schea):
        table_name=f"{database}.{schema}.{table}"
        if not self.session:
            self.Session()

        # Get column names from the DataFrame
        columns = df.columns.tolist()

        # Convert DataFrame to list of tuples
        data = df.values.tolist()

        # Create the SQL query for insertion
        insert_query = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )

        # Use execute_values for efficient batch insert
        extras.execute_values(self.session, insert_query, data)

        # Commit the transaction
        self.conn.commit()

        print(f"DataFrame written to {table_name} successfully.")
