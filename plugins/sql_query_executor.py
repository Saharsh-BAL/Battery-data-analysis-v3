
from semantic_kernel.functions import kernel_function

import pyodbc
import json
import os
from dotenv import load_dotenv
load_dotenv()


class SQLQueryExecutor:
    """
    Description: Get the result of a SQL query
    """
    def __init__(self, connection_string) -> None:
        self._connection_string = connection_string
    
    @staticmethod
    def __clean_sql_query__(sql_query):
        sql_query = sql_query.replace(";", "")
        sql_query = sql_query.replace("/n ", " ")

        return sql_query  
    
    @kernel_function(name="execute_sql_on_db",
                     description="This agent to execute the final SQL query on the sql database and return the result.",)
    def execute_sql_on_db(self, query: str) -> str:
        """
        Description: Execute a SQL query created by the sql_divide_conqure agents and return the result
                Args:
            query (str): SQL query to execute
        Returns:
            str: Result of the SQL query
        """
        # Clean the SQL query to prevent SQL injection attacks
        query = self.__clean_sql_query__(query)
        
        # Print the cleaned SQL query for debugging purposes
        print("Cleaned Query: ", query)

        # Execute the SQL query and return the result
        return self.__execute_query__(query)
    

    
    def __execute_query__(self,query) -> str:    
        print("Query: ", query)
        # Connect to the SQL Server database
        conn = pyodbc.connect(self._connection_string)

        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        
        try:
            cursor.execute(self.__clean_sql_query__(query))
            #result = cursor.fetchone()
            
            # Get the column names from cursor.description
            columns = [column[0] for column in cursor.description]

            # Initialize an empty list to store the results as dictionaries
            results = []

            # Fetch all rows and create dictionaries
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            print(results)
        except Exception as e:
            return f"Error: {e}"
        cursor.close()
        conn.close()
        
        return json.dumps(results)



# if __name__ == "__main__":
#     s= SQLQueryExecutor(os.getenv("SQL_CONNECTION_STRING"))
#     result = s.execute_sql_on_db("SELECT * FROM [dbo].[test]")  
#     print(result)