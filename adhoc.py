import json  
import sqlparse  
import pyodbc  
import pandas as pd  
from simple_ddl_parser import DDLParser  
  
def parse_create_table_statements(file_path, connection_string):  
    """  
    Parse the CREATE TABLE statements from the given SQL file using sqlparse and extract table and column details.  
    """  
    with open(file_path, 'r') as file:  
        sql_content = file.read()  
  
    # Parse the SQL schema using simple-ddl-parser  
    parser = DDLParser(sql_content)  
    parsed_schema = parser.run()  
    table_details = []  
  
    for table in parsed_schema:  
        table_name = table['table_name']  
        columns = {col['name']: col['type'] for col in table['columns']}  
        primary_keys = table.get('primary_key', [])  
        foreign_keys = table.get('foreign_keys', [])  
  
        tables_relationships = {              
            "primary_keys": primary_keys,  
            "foreign_keys": foreign_keys  
        }  
  
        # Get top 5 samples for the table  
        top_5_samples = get_top_5_samples(connection_string, table_name, columns)  
  
        # Append table details  
        table_details.append({  
            "Table Name": table_name,  
            "Top 5 Samples": top_5_samples,  
            "Relationships": tables_relationships  
        })  
  
    return table_details  
  
def get_top_5_samples(connection_string, table_name, columns):  
    """  
    Retrieve the top 5 samples from the specified table and return the output in the desired format.  
    """  
    try:  
        # Connect to SQL Server  
        conn = pyodbc.connect(connection_string)  
        cursor = conn.cursor()  
  
        # Query to get top 5 rows  
        query_top_5 = f"SELECT TOP 5 * FROM {table_name}"  
        df = pd.read_sql(query_top_5, conn)  
  
        # Convert Timestamp objects to strings  
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)  
  
        # Prepare the output  
        output = []  
        for col_name, col_data_type in columns.items():  
            column_name = col_name.replace("[", "").replace("]", "")  
            data_type = col_data_type.replace("[", "").replace("]", "")  
            sample_values = df[column_name].tolist() if column_name in df.columns else []  
            output.append({  
                "Column Name": column_name,  
                "Data Type": data_type,  
                "Sample Values": sample_values  
            })  
  
        # Close the connection  
        conn.close()  
  
        return output  
  
    except Exception as e:  
        print(f"Error: {e}")  
        return None   
  
# Path to the SQL file containing CREATE TABLE statements  
sql_file_path = "./sql-scripts/create-tables.sql"  
  
# Connection string to SQL Server  
connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:cleuranalytics.database.windows.net,1433;Database=analytics;Uid=analyticsdba;Pwd=3lectric!ty;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"  
  
# Parse the SQL file to extract table and column details  
table_details = parse_create_table_statements(sql_file_path, connection_string)  
  
# Convert the output to JSON format and print it  
print(json.dumps(table_details, indent=4))  



# import sqlparse  
# import pyodbc  
# import pandas as pd  
  
# from simple_ddl_parser import DDLParser
  
# def parse_create_table_statements(file_path,connection_string):  
#     """  
#     Parse the CREATE TABLE statements from the given SQL file using sqlparse and extract table and column details.  
#     """  
#     with open(file_path, 'r') as file:  
#         sql_content = file.read()  
  

#    # Parse the SQ  L schema using simple-ddl-parser
#         parser = DDLParser(sql_content)
#         parsed_schema = parser.run()
#         table_details = []

#         for table in parsed_schema:
#             table_name = table['table_name']
#             # for col in table['columns']:
#             #     column_name = col['name']
#             #     data_type = col['type']
#             #     query = f'select top 5 {column_name} from {table_name}'
#             #     table_details.append([table_name, column_name, data_type, query])
#             columns = {col['name']: col['type'] for col in table['columns']}
#             primary_keys = table.get('primary_key', [])
#             foreign_keys = table.get('foreign_keys', [])

#             tables_relationships = {
#                 "columns": columns,
#                 "primary_keys": primary_keys,
#                 "foreign_keys": foreign_keys
#             }

#             # Extract relationships from foreign keys
#             # for fk in foreign_keys:
#             #     relationships.append({
#             #         "table": table_name,
#             #         "column": fk['column'],
#             #         "references_table": fk['references']['table'],
#             #         "references_column": fk['references']['column']
#             #     })
#             table_details.append([get_top_5_samples(connection_string, table_name, columns), tables_relationships])
#                 # print(json.dumps(tables, indent=4))  




#     return table_details  
  
  
# def get_top_5_samples(connection_string, table_name, columns):  
#     """  
#     Retrieve the top 5 samples from the specified table and return the output in the desired format.  
#     """  
#     try:  
#         # Connect to SQL Server  
#         conn = pyodbc.connect(connection_string)  
#         cursor = conn.cursor()  

#         # Query to get top 5 rows  
#         query_top_5 = f"SELECT TOP 5 * FROM {table_name}"  
#         df = pd.read_sql(query_top_5, conn)  
  
#         # Prepare the output  
#         output = []  
#         for col_name, col_data_type in columns.items():  
#             column_name = col_name.replace("[", "").replace("]", "")  
#             data_type = col_data_type.replace("[", "").replace("]", "")  
#             sample_values = df[column_name].tolist() if column_name in df.columns else []  
#             output.append({  
#                 "Table Name": table_name,  
#                 "Column Name": column_name,  
#                 "Data Type": data_type,  
#                 "Sample Values": sample_values  
#             })  
  
#         # Close the connection  
#         conn.close()  
  
#         return output  
  
#     except Exception as e:  
#         print(f"Error: {e}")  
#         return None  
  
  
# # Path to the SQL file containing CREATE TABLE statements  
# sql_file_path = "./sql-scripts/create-tables.sql"  


# # Connection string to SQL Server  
# connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:cleuranalytics.database.windows.net,1433;Database=analytics;Uid=analyticsdba;Pwd=3lectric!ty;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"  
    
# # Parse the SQL file to extract table and column details  
# table_details = parse_create_table_statements(sql_file_path,connection_string)  
# print(table_details)
