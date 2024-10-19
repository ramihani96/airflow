import psycopg2
from psycopg2 import extras
import sys
from datetime import datetime, date
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import csv
from io import StringIO
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import mysql.connector
import psutil
import math
import gc


class data_management:
    def __init__(self):
        self.PARAM_DIC_DWH = self.get_connection_credentials('data_warehouse')

    def get_connection_credentials(self, conn_id):
        return {'database': BaseHook.get_connection(conn_id).schema,
                'user': BaseHook.get_connection(conn_id).login,
                'host': BaseHook.get_connection(conn_id).host,
                'password': BaseHook.get_connection(conn_id).password}

    def connect(self, database):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            PARAM_DIC = {}
            # Connect to the PostgreSQL server
            print(f'Connecting to the PostgreSQL database {database}...')
            if database == 'dwh':
                PARAM_DIC = self.PARAM_DIC_DWH
            # elif database == 'listing':
            #     PARAM_DIC = self.get_connection_credentials('listing_production')
            # else:
            #     PARAM_DIC = self.get_connection_credentials(f'{database}_production')
            
            # MySQL database connection
            # if database == 'listing':
            #     conn = mysql.connector.connect(database=PARAM_DIC['database'],
            #                                 user=PARAM_DIC['user'],
            #                                 host=PARAM_DIC['host'],
            #                                 password=PARAM_DIC['password'])
            # Postgres database connection
            # else:
            conn = psycopg2.connect(database=PARAM_DIC['database'], 
                                user=PARAM_DIC['user'],
                                host=PARAM_DIC['host'],
                                password=PARAM_DIC['password'])
        
            print(f"Connection successful to database --> {database}")
            return conn

        except (Exception, psycopg2.DatabaseError) as error:
            print("Database Connection Error: ", error)
            sys.exit(1)

    def copy_from_stringio(self, df, table, schema='public', operation='insert', chunk_size=400000):
        """
        Execute SQL statement inserting data

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        operation : Either 'insert' (insert rows) or 'create' (create the table if not exists and insert rows).
        chunk_size: number of rows to be inserted each time
        """
        # Connect to Data warehouse
        host = self.PARAM_DIC_DWH['host']
        user = self.PARAM_DIC_DWH['user']
        password = self.PARAM_DIC_DWH['password']
        database = self.PARAM_DIC_DWH['database']
        port = 5432
        # port = 3306 # mysql connection port
        
        postgres_conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}" 
        # mysql_conn_str = f"mysql+mysqldb://{user}:{password}@{host}:{port}/{database}"
        
        # Connect to the database
        con = create_engine(postgres_conn_str, pool_recycle=3600, pool_size=20, pool_pre_ping=True)
        
        if operation == 'create':
            # Create table if it doesn't exist
            df.head(0).to_sql(table, con=con, schema=schema, index=False, if_exists='replace') # head(0) uses only the header
            
        df = df.replace({np.nan: None})

        # Convert dataframe into a CSV
        dbapi_conn = con.raw_connection()
        for chunk in range(0, len(df), chunk_size):
            df_chunk = df.iloc[chunk:chunk + chunk_size]
            data_iter = [list(i) for i in df_chunk.values]

            with dbapi_conn.cursor() as cur:
                s_buf = StringIO()
                writer = csv.writer(s_buf)
                writer.writerows(data_iter)
                s_buf.seek(0)

                columns = ', '.join(['"{}"'.format(k) for k in df_chunk.columns])

                sql = f'COPY {schema}.{table} ({columns}) FROM STDIN WITH CSV'
                cur.copy_expert(sql=sql, file=s_buf)
                dbapi_conn.commit()

        # Close connection and clear resources
        dbapi_conn.close()
        con.dispose()

    def execute_sql(self, sql_query, database):
        """ Execute any SQL query on a specified database """
        try:
            conn = self.connect(database)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"The following query executed successfully on {database} database. \n {sql_query}")

        except Exception as error:
            print(f"Error executing query: {error}")
            raise

    def insert_update_on_conflict(self, df, table, schema='public', array_columns=[], 
                                array_columns_data_types=[], json_cols=[], unique_keys=["id"], 
                                date_cols=[], float_cols = [], bool_cols=[], int_cols=[], chunk_size=400000):
        """
            Function to perform upsert operation handling special column types (E.G. Arrays, json ...)
        """
        print(f"Checkpoint start upsert function: --> {self.get_available_memory()}")
        dwh_conn = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
                    self.PARAM_DIC_DWH['user'],
                    self.PARAM_DIC_DWH['password'],
                    self.PARAM_DIC_DWH['host'],
                    self.PARAM_DIC_DWH['database']
                )
        con = create_engine(dwh_conn, pool_recycle=3600, pool_size=20)
        
        # Replace empty strings of text columns
        text_cols = df.select_dtypes(include=['object', 'boolean']).columns
        for col in text_cols:
            df[col] = df[col].replace({np.nan: None})
            df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.lower() in ['', 'nan', 'null'] else x)
            
        # Replace pd.NA with None
        integer_columns = df.select_dtypes(include=['Int64']).columns
        for col in integer_columns:
            df[col] = df[col].replace({pd.NA: None})

        # Replace pd.NaT only for datetime columns
        datetime_columns = df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]', 'datetime64[ns, Africa/Cairo]','datetime64']).columns
        for col in datetime_columns:
            df[col] = df[col].replace({pd.NaT: pd.Timestamp('2099-12-31 00:00:00')})

        # Convert array columns
        for col in array_columns:
            df[col] = df[col].apply(lambda x: '{{{}}}'.format(','.join(map(str, x))) if isinstance(x, list) else None)

        # Dictionary for mapping data types from python to postgresql
        mapping_data_types = {'int64': 'int8',
                            'Int64': 'int8',
                            'object': 'text',
                            'datetime64[ns]': 'timestamp',
                            'datetime64[ns, UTC]': 'timestamp with time zone',
                            'datetime64[us, UTC]': 'timestamp with time zone',
                            'datetime64[ns, Africa/Cairo]': 'timestamp',
                            'datetime64[s]' :'timestamp',
                            'datetime64[us]': 'timestamp',
                            'date': 'date',
                            'dbdate': 'date',
                            'dict': 'jsonb',
                            'float64': 'numeric',
                            'bool': 'boolean',
                            'boolean': 'boolean'}

        print("dataframe data types to be inserted: ", df.dtypes)
        rows, cols = df.shape
        print("Number of rows: ", rows)
        print("Number of columns", cols)

        dbapi_conn = con.raw_connection()

        for chunk in range(0, len(df), chunk_size): 
            df_chunk = df.iloc[chunk:chunk + chunk_size]
            data_iter = [list(i) for i in df_chunk.values]

            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join(['"{}"'.format(k) for k in df_chunk.columns])
            table_name = schema + '.' + table
            temp_table_name = 'temp_' + table

            # Create temporary table
            temp_table_sql = f'CREATE TEMP TABLE {temp_table_name} AS SELECT * FROM {table_name} WHERE 1=0;'
            with dbapi_conn.cursor() as cur:
                cur.execute(temp_table_sql)
                dbapi_conn.commit()

                data_types = [
                    'date' if col in date_cols
                        else 'float8' if col in float_cols
                        else 'int8' if col in int_cols
                        else 'bool' if col in bool_cols
                        else mapping_data_types[str(df_chunk[col].dtype)] 
                    for col in df_chunk.columns] 

                # Generate the column part of the COPY statement with casting
                columns_with_types = ', '.join([
                    f'NULLIF("{col}", \'NaN\')::{data_type}' if data_type in ['numeric', 'float8'] else
                    f'NULLIF("{col}", \'2099-12-31 00:00:00\')::{data_type}' if data_type in ['timestamp', 'timestamptz', 'timestamp without time zone', 'date'] else
                    f'"{col}"::jsonb' if col in json_cols else
                    f'"{col}"::{array_columns_data_types[array_columns.index(col)]}[]' if col in array_columns else
                    f'"{col}"::{data_type}'
                    for col, data_type in zip(df_chunk.columns, data_types) 
                ])

                sql = f'COPY {temp_table_name} ({columns}) FROM STDIN WITH CSV'
                print("Copy expert query : ", sql)
                cur.copy_expert(sql=sql, file=s_buf)
                dbapi_conn.commit()
                self.clear_memory(df=df_chunk)

            # Dynamic formatting of unique keys for the ON CONFLICT statement
            conflict_keys = ', '.join(['"{}"'.format(k) for k in unique_keys])
            sql = f'INSERT INTO {table_name} ({columns}) SELECT {columns_with_types} FROM {temp_table_name} ON CONFLICT ({conflict_keys}) DO UPDATE SET '

            # Generate the column update assignments for the DO UPDATE part
            update_assignments = ', '.join(['"{0}"=EXCLUDED."{0}"'.format(k) for k in df_chunk.columns if k not in unique_keys])
            sql += update_assignments

            # Generate the where condition statement
            where_conditions = ' WHERE ' + ' OR '.join([f'{table_name}."{col}" IS DISTINCT FROM EXCLUDED."{col}"' if col not in json_cols else f'CAST({table_name}."{col}" AS TEXT) IS DISTINCT FROM CAST(EXCLUDED."{col}" AS TEXT)' for col in df_chunk.columns if col not in unique_keys])
            sql += where_conditions

            # Execute the SQL statement with the data from s_buf using executemany
            print("Query to be executed: ", sql)

            with dbapi_conn.cursor() as cur:
                cur.execute(sql)

                # Fetch the number of affected rows
                affected_rows = cur.rowcount
                print(f'Numbers of rows affected: {affected_rows}')
                drop_temp_table = f'DROP TABLE {temp_table_name};'
                cur.execute(drop_temp_table)
                print(f'{temp_table_name} dropped successfully!')
                dbapi_conn.commit()

        # Close connection and clear resources
        dbapi_conn.close()
        con.dispose()
        self.clear_memory(df=df)
        
    def postgresql_to_dataframe(self, conn, select_query):
        """
        Tranform a SELECT query into a pandas dataframe
        
        Parameters:
        - conn: The database connection.
        - select_query: The SELECT query to execute.
        
        Returns:
        - DataFrame: Pandas DataFrame containing the results.
        """
        cursor = conn.cursor()
        try:
            cursor.execute(select_query)
            # Get column names from cursor description
            field_names = [i[0] for i in cursor.description if len(i) > 0]
        except (Exception, psycopg2.DatabaseError, mysql.connector.Error) as error:
            print(f"Error: {error}")
            cursor.close()
            return pd.DataFrame(columns=field_names)  # Return an empty DataFrame with column names in case of an error

        # Fetch all rows
        tupples = cursor.fetchall()
        
        if not tupples:
            cursor.close()
            return pd.DataFrame(columns=field_names)  # Return an empty DataFrame with column names if no rows are returned
        
        # Create a DataFrame from the fetched rows
        df = pd.DataFrame(tupples, columns=field_names)
        
        cursor.close()
        return df
                
                
    def postgresql_to_chunks(self, conn, select_query, chunk_size=400000):
        """
        Tranform a SELECT query into a pandas dataframe
        
        Parameters:
        - conn: The database connection.
        - select_query: The SELECT query to execute.
        - chunk_size: The number of rows to fetch per chunk. If -1, fetch all at once.
        
        Yields:
        - DataFrame: Pandas DataFrame containing the results.
        """
        cursor = conn.cursor()
        
        try:
            # Count total chunks
            cursor.execute(select_query)
            total_rows = cursor.rowcount
            total_chunks = math.ceil(total_rows / chunk_size)
            cursor.close()

            # Yield chunks
            for chunk in pd.read_sql_query(select_query, conn, chunksize=chunk_size):
                yield chunk, total_chunks

        except (Exception, psycopg2.DatabaseError, mysql.connector.Error) as error:
            raise RuntimeError(f"Error executing query: {error}")

        finally:
            cursor.close()
                

    def get_table_columns(self, database_name, table_name, schema_name='public', exclude_columns=[]):
        # Connect to the PostgreSQL database
        conn = self.connect(database=database_name)        
        cursor = conn.cursor()

        # Execute a query to retrieve column names
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = %s
            ORDER BY ordinal_position
        """
        cursor.execute(query, (table_name, schema_name))

        # Fetch all column names except the excluded ones
        columns = [ column[0]  for column in cursor.fetchall() if column[0] not in exclude_columns]
        print("columns: ", columns)

        # Close the cursor and database connection
        cursor.close()
        conn.close()

        return columns
   
    def add_missing_columns_to_table(self, df, table_name, schema_name):
        """
        A function to alter table schema to handle columns that are in the dataframe and not in the DWH table 
        
        Args:
        df (pd.DataFrame): The dataframe containing the new data.
        table_name (str): The name of the table in the DWH.
        schema_name (str): The name of the schema in the DWH.
        """
        df_column_names = list(df.columns)
        dwh_column_names = self.get_table_columns(database_name='dwh', table_name=table_name, schema_name=schema_name)
        dwh_column_names = [column.strip('"') for column in dwh_column_names] # Remove double quotes from DWH column names
        new_columns = list(set(df_column_names)-set(dwh_column_names))
        print(f"New Columns to be added: {new_columns}")
        
        if new_columns:
            print(f'column {new_columns} is to be added to the DWH in table {schema_name}.{table_name} !')
            
            # Constructing a single ALTER TABLE statement to add all new columns
            alter_table_query = f"""ALTER TABLE {schema_name}.{table_name} """
            alter_table_query += ", ".join([f'ADD COLUMN IF NOT EXISTS "{column}" TEXT' for column in new_columns])
            print("alter_table_query: ", alter_table_query)

            self.execute_sql(sql_query=alter_table_query, database='dwh')
        
    def get_memory_usage_gb(self):
        process = psutil.Process()
        mem_info = process.memory_full_info()
        return mem_info.uss / (1024 ** 3)  # USS (Unique Set Size) is more accurate for your case

    
    def clear_memory(self, df):
        """
        Clears memory occupied by a DataFrame using del and gc.collect().

        Parameters:
        df (pd.DataFrame): DataFrame to be cleared from memory.
        """
        print(f"Memory usage before clearing: {self.get_available_memory()} GB")
        del df
        gc.collect()
        print(f"Memory usage after clearing: {self.get_available_memory()} GB")
    
    def truncate_tables_list(self, schema_name, tables_list=[]):
        for table_name in tables_list:
            self.execute_sql(sql_query=f"TRUNCATE TABLE {schema_name}.{table_name}", database='dwh')
            print(f"{table_name} truncated successfully !")
        pass
    
    
    def my_sql_to_postgres_data_types_mapping(self, my_sql_data_type):
        """
        A function to returns mysql data types equivalent mapping in postgres database 
        """
        my_sql_to_postgres_type_mapping = {
            'bigint': 'bigint',
            'varchar': 'character varying',
            'text': 'text',
            'datetime': 'timestamp without time zone',
            'json': 'jsonb',
            'tinyint': 'smallint',
            'int': 'integer',
            'double': 'double precision',
            'date': 'date',
            'timestamp': 'timestamp without time zone',
            'enum': 'text',
            'decimal': 'numeric',
            'smallint': 'smallint',
            'float': 'real',
            'blob': 'bytea',
            'longtext': 'text',
            'mediumtext': 'text',
            'set': 'text',
            'binary': 'bytea',
            'char': 'character',
            'varbinary': 'bytea',
        }
        
        return my_sql_to_postgres_type_mapping.get(my_sql_data_type)
        
    
    def sync_schemas(self, source_schema_name, destination_schema_name, tables_list, 
                     source_database, destination_database='dwh', source_db_type='postgres'):
        """
        Synchronizes the schema between source and destination databases by comparing columns
        and data types, and then performs necessary changes.

        Args:
        source_schema_name (str): Schema name in the source database.
        destination_schema_name (str): Schema name in the destination database.
        tables_list (list): List of table names to validate.
        source_database (str): Source database connection ID.
        destination_database (str): Destination database connection ID. Defaults to 'dwh'.
        source_db_type (str): Source database type ('mysql' or 'postgres'). Defaults to 'postgres'.
        """
        SOURCE_CONN = None
        DESTINATION_CONN = None
        try:
            # Connect to the databases
            SOURCE_CONN = self.connect(source_database)
            DESTINATION_CONN = self.connect(destination_database)
            
            print(f"The list of tables to be checked --> {tables_list}")
            
            for table_name in tables_list:
                print(f"Validating schema for table: {table_name}")

                # Retrieve column names and data types from the source database
                source_columns_query = f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    AND table_schema = '{source_schema_name}'
                    order by column_name;
                """
                source_df = self.postgresql_to_dataframe(SOURCE_CONN, source_columns_query)
                source_df.columns = [col.lower() for col in source_df.columns]


                if source_df.empty:
                    print(f"No columns found in the source schema for table: {table_name}")
                    continue  # Skip to the next table if source DataFrame is empty

                # Retrieve column names and data types from the destination database
                destination_columns_query = f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    AND table_schema = '{destination_schema_name}'
                    order by column_name;
                """
                destination_df = self.postgresql_to_dataframe(DESTINATION_CONN, destination_columns_query)
                
                
                if destination_df.empty:
                    print(f"No columns found in the destination schema for table: {table_name}")
                    continue  # Skip to the next table if destination DataFrame is empty
                

                if source_db_type == 'mysql':
                    # Mapping MySQL data types to PostgreSQL data types in the source schema
                    source_df['mapped_data_type'] = source_df['data_type'].apply(self.my_sql_to_postgres_data_types_mapping)
                    print("mapped data type successfully fetched")
                else:
                    source_df['mapped_data_type'] = source_df['data_type']
                    print("da5al fel else mapped data type successfully fetched")

                # Identifying columns to add, modify, or drop in the destination schema
                columns_to_add = []
                # columns_to_modify = []
                columns_to_drop = []

                source_columns_set = set(source_df['column_name'])
                print("source_columns_set: ", source_columns_set)
                destination_columns_set = set(destination_df['column_name'])
                print("destination columns set: ", destination_columns_set)

                for index, row in source_df.iterrows():
                    source_column = row['column_name']
                    print("row[col_name] okay")
                    source_data_type = row['mapped_data_type']
                    print("row[mapped_data_type] okay")
                    destination_row = destination_df[destination_df['column_name'] == source_column]
                    
                    if destination_row.empty:
                        # Column does not exist in the destination table, so add it
                        columns_to_add.append((source_column, source_data_type))
                        print(f"adding column {source_column} with {source_data_type} to columns_to_add")
                    # else:
                        # destination_data_type = destination_row.iloc[0]['data_type']
                        # if source_data_type != destination_data_type:
                            # Column data type differs between source and destination tables, so modify it
                            # columns_to_modify.append((source_column, source_data_type))

                for destination_column in destination_columns_set:
                    if destination_column not in source_columns_set:
                        # Column exists in the destination table but not in the source table, so drop it
                        columns_to_drop.append(destination_column)
                
                print("Columns to add:", columns_to_add)
                # print("Columns to modify:", columns_to_modify)
                print("Columns to drop:", columns_to_drop)

                # Constructing ALTER TABLE statements to add, modify, or drop columns in the destination table
                alter_statements = []
                
                if columns_to_add:
                    add_columns_query = f"""ALTER TABLE {destination_schema_name}.{table_name} """
                    add_columns_query += ", ".join([f'ADD COLUMN IF NOT EXISTS "{col[0]}" {col[1]}' for col in columns_to_add])
                    alter_statements.append(add_columns_query)
                
                # if columns_to_modify:
                    # modify_columns_query = f"""ALTER TABLE {destination_schema_name}.{table_name} """
                    # modify_columns_query += ", ".join([f'ALTER COLUMN "{col[0]}" TYPE {col[1]} USING "{col[0]}"::{col[1]}' for col in columns_to_modify])
                    # alter_statements.append(modify_columns_query)

                if columns_to_drop:
                    drop_columns_query = f"""ALTER TABLE {destination_schema_name}.{table_name} """
                    drop_columns_query += ", ".join([f'DROP COLUMN IF EXISTS "{col}"' for col in columns_to_drop])
                    alter_statements.append(drop_columns_query)

                # Execute ALTER TABLE statements
                for statement in alter_statements:
                    print(f"Executing statement --> {statement}")
                    self.execute_sql(sql_query=statement, database=destination_database)

                print(f"Schema synchronization completed for table {destination_schema_name}.{table_name}.")

        except Exception as e:
            print(f"An error occurred during schema synchronization: {e}")
        
        finally:
            # Close database connections
            if SOURCE_CONN:
                SOURCE_CONN.close()
            if DESTINATION_CONN:
                DESTINATION_CONN.close()

        print("Schema validation and synchronization completed for all tables.")

        
    def get_available_memory(self):
        available_memory_gb = psutil.virtual_memory().available / (1024 ** 3)
        return available_memory_gb
