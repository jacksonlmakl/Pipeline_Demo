# import psycopg2
# from psycopg2 import sql,extras
# import pandas as pd
# from psycopg2.extensions import register_type, UNICODE, UNICODEARRAY

# class Connection:
#     def __init__(self,id,host,port,username,password,database):
#         self.id = id
#         self.host = host
#         self.port = port
#         self.username = username
#         self.password = password
#         self.session = None
#         self.conn=None
#         self.database=database
#     def Session(self):
#         db_config = {
#             'user': self.username,
#             'password': self.password,
#             'host': self.host,  # Example: 'localhost' or an IP address
#             'port': self.port,   # Example: '5432' is the default PostgreSQL port
#             'database':self.database
#         }
#         self.conn = psycopg2.connect(**db_config)
#         self.session = self.conn.cursor()
#     def close(self):
#         if self.session:
#             self.session.close()
#         # if self.conn:
#         #     self.conn.close()
#     def query(self,code):
#         if not self.session:
#             self.Session()
#         self.session.execute(sql.SQL(code))
#         result = self.session
#         return result
#     def query_to_df(self,code):
#         if not self.session:
#             self.Session()
#         self.session.execute(sql.SQL(code))
#         result = self.session.fetchall()
#         col_names = [desc[0] for desc in self.session.description]

#         # Create a DataFrame from the result
#         df = pd.DataFrame(result, columns=col_names)
#         return df


#     def df_to_table(self, df, table, database, schema, materialization_type, schema_change_behavior='drop_and_recreate', primary_key=None):
#         table_name = f"{schema}.{table}"
    
#         if not self.session:
#             self.Session()
    
#         # Get column names and data from DataFrame
#         columns = df.columns.tolist()
#         data = df.values.tolist()
    
#         # Create schema if it does not exist
#         create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
#             sql.Identifier(schema)
#         )
#         self.session.execute(create_schema_query)
    
#         # Check if table exists
#         table_exists_query = sql.SQL("""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables 
#                 WHERE table_schema = %s AND table_name = %s
#             )
#         """)
#         self.session.execute(table_exists_query, (schema, table))
#         table_exists = self.session.fetchone()[0]
    
#         # If table exists, check schema
#         if table_exists:
#             # Retrieve the current table schema
#             get_table_schema_query = sql.SQL("""
#                 SELECT column_name, data_type 
#                 FROM information_schema.columns 
#                 WHERE table_schema = %s AND table_name = %s
#             """)
#             self.session.execute(get_table_schema_query, (schema, table))
#             existing_schema = self.session.fetchall()
    
#             # Convert existing schema into a dictionary for easier comparison
#             existing_schema_dict = {col: dtype for col, dtype in existing_schema}
    
#             # Assuming all DataFrame columns are TEXT by default (adjust as necessary)
#             df_schema_dict = {col: 'TEXT' for col in columns}
    
#             # Check for schema differences
#             schema_diff = df_schema_dict != existing_schema_dict
            
#             if schema_diff:
#                 if schema_change_behavior == 'drop_and_recreate':
#                     # Drop and recreate the table
#                     drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
#                         sql.Identifier(schema),
#                         sql.Identifier(table)
#                     )
#                     self.session.execute(drop_table_query)
    
#                     # Create the new table
#                     create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
#                         sql.Identifier(schema),
#                         sql.Identifier(table),
#                         sql.SQL(', ').join(
#                             sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
#                         )
#                     )
#                     self.session.execute(create_table_query)
    
#                 elif schema_change_behavior == 'error':
#                     raise ValueError(f"Schema mismatch detected between DataFrame and existing table {table_name}. Aborting.")
    
#         elif not table_exists:
#             # Create table if it does not exist
#             create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(', ').join(
#                     sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
#                 )
#             )
#             self.session.execute(create_table_query)
    
#         # Handle materialization_type logic
#         if materialization_type == 'incremental':
#             if primary_key is None:
#                 raise ValueError("Primary key is required for incremental materialization.")
            
#             # Perform insert/update using primary_key
#             update_query = sql.SQL("""
#                 INSERT INTO {}.{} ({}) 
#                 VALUES %s
#                 ON CONFLICT ({}) DO UPDATE 
#                 SET {}
#             """).format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(', ').join(map(sql.Identifier, columns)),
#                 sql.Identifier(primary_key),
#                 sql.SQL(', ').join(
#                     sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if col != primary_key
#                 )
#             )
#             extras.execute_values(self.session, update_query, data)
    
#         elif materialization_type == 'truncate':
#             # Truncate and insert all data
#             truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table)
#             )
#             self.session.execute(truncate_query)
    
#             insert_query = sql.SQL('INSERT INTO {}.{} ({}) VALUES %s').format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(', ').join(map(sql.Identifier, columns))
#             )
#             extras.execute_values(self.session, insert_query, data)
    
#         elif materialization_type == 'temp':
#             # Create temp table and insert all data
#             temp_table_name = f"temp_{table}"
#             create_temp_table_query = sql.SQL("CREATE TEMP TABLE {} ({})").format(
#                 sql.Identifier(temp_table_name),
#                 sql.SQL(', ').join(
#                     sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
#                 )
#             )
#             self.session.execute(create_temp_table_query)
    
#             insert_query = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
#                 sql.Identifier(temp_table_name),
#                 sql.SQL(', ').join(map(sql.Identifier, columns))
#             )
#             extras.execute_values(self.session, insert_query, data)
    
#         elif materialization_type == 'None':
#             # Simply return the DataFrame
#             return df
    
#         # Commit the transaction
#         self.conn.commit()
    
#         print(f"DataFrame written to {table_name} successfully.")

#     def query_to_table(self, query, table, database, schema, materialization_type, schema_change_behavior='drop_and_recreate', primary_key=None):
#         table_name = f"{schema}.{table}"
    
#         if not self.session:
#             self.Session()
    
#         # Create schema if it does not exist
#         create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
#             sql.Identifier(schema)
#         )
#         self.session.execute(create_schema_query)
    
#         # Check if table exists
#         table_exists_query = sql.SQL("""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables 
#                 WHERE table_schema = %s AND table_name = %s
#             )
#         """)
#         self.session.execute(table_exists_query, (schema, table))
#         table_exists = self.session.fetchone()[0]
    
#         # If table exists, check schema
#         if table_exists:
#             query_schema_query = f"""
#                 SELECT * FROM "{schema}"."{table}" AS subquery LIMIT 0;
#             """
#             self.session.execute(query_schema_query)
#             existing_schema = [(desc[0], psycopg2.extensions.string_types[desc[1]].name) for desc in self.session.description]  # Get column names and data types
            
#             query_schema_query = f"""
#                 SELECT * FROM ({query}) AS subquery LIMIT 0;
#             """
#             self.session.execute(query_schema_query)
#             query_schema = [(desc[0], psycopg2.extensions.string_types[desc[1]].name) for desc in self.session.description]  # Get column names and data types
            
#             existing_schema_dict = {col: dtype for col, dtype in existing_schema}
#             query_schema_dict = {col: dtype for col, dtype in query_schema}
    
#             schema_diff = query_schema_dict != existing_schema_dict
#             if schema_diff:
#                 if schema_change_behavior == 'drop_and_recreate':
#                     drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
#                         sql.Identifier(schema),
#                         sql.Identifier(table)
#                     )
#                     self.session.execute(drop_table_query)
    
#                     create_table_query = sql.SQL("CREATE TABLE {}.{} AS ({})").format(
#                         sql.Identifier(schema),
#                         sql.Identifier(table),
#                         sql.SQL(query)
#                     )
#                     self.session.execute(create_table_query)
    
#                 elif schema_change_behavior == 'error':
#                     raise ValueError(f"Schema mismatch detected between query and existing table {table_name}. Aborting.")
    
#         elif not table_exists:
#             create_table_query = sql.SQL("CREATE TABLE {}.{} AS ({})").format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(query)
#             )
#             self.session.execute(create_table_query)
    
#         # Handle materialization_type logic
#         if materialization_type == 'incremental':
#             if primary_key is None:
#                 raise ValueError("Primary key is required for incremental materialization.")
#             self.session.close()
#             try:
#                 self.Session()
#                 create_primary = sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({});").format(
#                     sql.Identifier(schema),
#                     sql.Identifier(table),
#                     sql.Identifier(primary_key),
#                 )
#                 self.session.execute(create_primary)
#                 self.session.close()
                
#             except:
#                 self.Session()

#             update_query = sql.SQL("""
#                 INSERT INTO {}.{} ({})
#                 SELECT * FROM ({}) AS subquery
#                 ON CONFLICT ({}) DO UPDATE 
#                 SET {}
#             """).format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(', ').join(map(sql.Identifier, query_schema_dict.keys())),
#                 sql.SQL(query),
#                 sql.Identifier(primary_key),
#                 sql.SQL(', ').join(
#                     sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in query_schema_dict.keys() if col != primary_key
#                 )
#             )
#             self.session.execute(update_query)
    
#         elif materialization_type == 'truncate':
#             truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table)
#             )
#             self.session.execute(truncate_query)
    
#             insert_query = sql.SQL("""
#                 INSERT INTO {}.{} ({})
#                 SELECT * FROM ({}) AS subquery
#             """).format(
#                 sql.Identifier(schema),
#                 sql.Identifier(table),
#                 sql.SQL(', ').join(map(sql.Identifier, query_schema_dict.keys())),
#                 sql.SQL(query)
#             )
#             self.session.execute(insert_query)
    
#         elif materialization_type == 'temp':
#             temp_table_name = f"temp_{table}"
#             create_temp_table_query = sql.SQL("CREATE TEMP TABLE {} AS ({})").format(
#                 sql.Identifier(temp_table_name),
#                 sql.SQL(query)
#             )
#             self.session.execute(create_temp_table_query)
    
#         elif materialization_type == 'None':
#             self.session.execute(query)
#             result = self.session.fetchall()
#             return result
    
#         self.conn.commit()
    
#         print(f"Query results written to {table_name} successfully.")
import psycopg2
from psycopg2 import sql, extras
import pandas as pd
from psycopg2.extensions import register_type, UNICODE, UNICODEARRAY

class Connection:
    def __init__(self, id, host, port, username, password, database):
        self.id = id
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.session = None
        self.conn = None
        self.database = database

    def Session(self):
        db_config = {
            'user': self.username,
            'password': self.password,
            'host': self.host,
            'port': self.port,
            'database': self.database
        }
        self.conn = psycopg2.connect(**db_config)
        self.session = self.conn.cursor()

    def close(self):
        if self.session:
            self.session.close()

    def query(self, code):
        if not self.session:
            self.Session()
        self.session.execute(sql.SQL(code))
        result = self.session
        return result

    def query_to_df(self, code):
        if not self.session:
            self.Session()
        self.session.execute(sql.SQL(code))
        result = self.session.fetchall()
        col_names = [desc[0] for desc in self.session.description]

        # Create a DataFrame from the result
        df = pd.DataFrame(result, columns=col_names)
        return df

    def df_to_table(self, df, table, database, schema, materialization_type, schema_change_behavior='drop_and_recreate', primary_key=None):
        table_name = f"{schema}.{table}"

        if not self.session:
            self.Session()

        # Get column names and data from DataFrame
        columns = df.columns.tolist()
        data = df.values.tolist()

        # Create schema if it does not exist
        create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema)
        )
        self.session.execute(create_schema_query)

        # Check if table exists
        table_exists_query = sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """)
        self.session.execute(table_exists_query, (schema, table))
        table_exists = self.session.fetchone()[0]

        # If table exists, check schema
        if table_exists:
            # Retrieve the current table schema
            get_table_schema_query = sql.SQL("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """)
            self.session.execute(get_table_schema_query, (schema, table))
            existing_schema = self.session.fetchall()

            # Convert existing schema into a dictionary for easier comparison
            existing_schema_dict = {col: dtype for col, dtype in existing_schema}

            # Assuming all DataFrame columns are TEXT by default (adjust as necessary)
            df_schema_dict = {col: 'TEXT' for col in columns}

            # Check for schema differences
            schema_diff = df_schema_dict != existing_schema_dict

            if schema_diff:
                if schema_change_behavior == 'drop_and_recreate':
                    # Drop and recreate the table
                    drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table)
                    )
                    self.session.execute(drop_table_query)

                    # Create the new table
                    create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.SQL(', ').join(
                            sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
                        )
                    )
                    self.session.execute(create_table_query)

                elif schema_change_behavior == 'error':
                    raise ValueError(f"Schema mismatch detected between DataFrame and existing table {table_name}. Aborting.")

        elif not table_exists:
            # Create table if it does not exist
            create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(
                    sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
                )
            )
            self.session.execute(create_table_query)

        # Handle materialization_type logic
        if materialization_type == 'incremental':
            if primary_key is None:
                raise ValueError("Primary key is required for incremental materialization.")

            # Perform insert/update using primary_key
            update_query = sql.SQL("""
                INSERT INTO {}.{} ({}) 
                VALUES %s
                ON CONFLICT ({}) DO UPDATE 
                SET {}
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.Identifier(primary_key),
                sql.SQL(', ').join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if col != primary_key
                )
            )
            extras.execute_values(self.session, update_query, data)

        elif materialization_type == 'truncate':
            # Truncate and insert all data
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            self.session.execute(truncate_query)

            insert_query = sql.SQL('INSERT INTO {}.{} ({}) VALUES %s').format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            extras.execute_values(self.session, insert_query, data)

        elif materialization_type == 'temp':
            # Create temp table and insert all data
            temp_table_name = f"temp_{table}"
            create_temp_table_query = sql.SQL("CREATE TEMP TABLE {} ({})").format(
                sql.Identifier(temp_table_name),
                sql.SQL(', ').join(
                    sql.SQL('{} {}').format(sql.Identifier(col), sql.SQL('TEXT')) for col in columns  # Assuming TEXT, modify this as needed
                )
            )
            self.session.execute(create_temp_table_query)

            insert_query = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
                sql.Identifier(temp_table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            extras.execute_values(self.session, insert_query, data)

        elif materialization_type == 'None':
            # Simply return the DataFrame
            return df

        # Commit the transaction
        self.conn.commit()

        print(f"DataFrame written to {table_name} successfully.")

    def query_to_table(self, query, table, database, schema, materialization_type, schema_change_behavior='drop_and_recreate', primary_key=None):
        table_name = f"{schema}.{table}"

        if not self.session:
            self.Session()

        # Create schema if it does not exist
        create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema)
        )
        self.session.execute(create_schema_query)

        # Check if table exists
        table_exists_query = sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """)
        self.session.execute(table_exists_query, (schema, table))
        table_exists = self.session.fetchone()[0]

        # If table exists, check schema
        if table_exists:
            query_schema_query = f"""
                SELECT * FROM "{schema}"."{table}" AS subquery LIMIT 0;
            """
            self.session.execute(query_schema_query)
            existing_schema = [(desc[0], psycopg2.extensions.string_types[desc[1]].name) for desc in self.session.description]  # Get column names and data types

            query_schema_query = f"""
                SELECT * FROM ({query}) AS subquery LIMIT 0;
            """
            self.session.execute(query_schema_query)
            query_schema = [(desc[0], psycopg2.extensions.string_types[desc[1]].name) for desc in self.session.description]  # Get column names and data types

            existing_schema_dict = {col: dtype for col, dtype in existing_schema}
            query_schema_dict = {col: dtype for col, dtype in query_schema}

            schema_diff = query_schema_dict != existing_schema_dict
            if schema_diff:
                if schema_change_behavior == 'drop_and_recreate':
                    drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table)
                    )
                    self.session.execute(drop_table_query)

                    create_table_query = sql.SQL("CREATE TABLE {}.{} AS ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.SQL(query)
                    )
                    self.session.execute(create_table_query)

                elif schema_change_behavior == 'error':
                    raise ValueError(f"Schema mismatch detected between query and existing table {table_name}. Aborting.")

        elif not table_exists:
            create_table_query = sql.SQL("CREATE TABLE {}.{} AS ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(query)
            )
            self.session.execute(create_table_query)

        # Handle materialization_type logic
        if materialization_type == 'incremental':
            if primary_key is None:
                raise ValueError("Primary key is required for incremental materialization.")

            # Check if primary key already exists in the table
            primary_key_exists_query = sql.SQL("""
                SELECT COUNT(*) 
                FROM information_schema.table_constraints 
                WHERE table_schema = %s 
                AND table_name = %s 
                AND constraint_type = 'PRIMARY KEY'
            """)
            self.session.execute(primary_key_exists_query, (schema, table))
            primary_key_exists = self.session.fetchone()[0] > 0

            # Add primary key if it doesn't exist
            if not primary_key_exists:
                try:
                    create_primary = sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({});").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(primary_key),
                    )
                    self.session.execute(create_primary)
                except psycopg2.Error as e:
                    # Ignore if the primary key already exists
                    if "already exists" not in str(e):
                        raise e

            # Perform insert/update using the query result and primary_key
            update_query = sql.SQL("""
                INSERT INTO {}.{} ({})
                SELECT * FROM ({}) AS subquery
                ON CONFLICT ({}) DO UPDATE 
                SET {}
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, query_schema_dict.keys())),  # Use the query columns
                sql.SQL(query),  # The query that generates data
                sql.Identifier(primary_key),
                sql.SQL(', ').join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in query_schema_dict.keys() if col != primary_key
                )
            )
            self.session.execute(update_query)

        elif materialization_type == 'truncate':
            # Truncate and insert all data from the query
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            self.session.execute(truncate_query)

            insert_query = sql.SQL("""
                INSERT INTO {}.{} ({})
                SELECT * FROM ({}) AS subquery
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, query_schema_dict.keys())),  # Use the query columns
                sql.SQL(query)  # The query that generates data
            )
            self.session.execute(insert_query)

        elif materialization_type == 'temp':
            # Create a temporary table and insert the query result
            temp_table_name = f"temp_{table}"
            create_temp_table_query = sql.SQL("CREATE TEMP TABLE {} AS ({})").format(
                sql.Identifier(temp_table_name),
                sql.SQL(query)  # Use the query to create the temp table
            )
            self.session.execute(create_temp_table_query)

        elif materialization_type == 'None':
            # Just run the query and return the result
            self.session.execute(query)
            result = self.session.fetchall()
            return result

        # Commit the transaction
        self.conn.commit()

        print(f"Query results written to {table_name} successfully.")
