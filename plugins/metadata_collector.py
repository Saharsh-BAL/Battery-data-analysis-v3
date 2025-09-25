import sqlparse  
from semantic_kernel import Kernel  

from semantic_kernel.functions import kernel_function

import pyodbc

  
class MetadataCollector:  
    '''
    A class to collect metadata from SQL files.
    It reads SQL files, extracts table definitions.
    '''

    def __init__(self, sql_file_path):  
        self.sql_file_path = sql_file_path  
  
    
    @kernel_function(name="read_table_definitions",
                     description= "Reads an SQL file and extracts table definitions and Uses the LLM to describe the table definition.")    
    def parse_sql_file(self ) -> str:  
        "Reads an SQL file and extracts table definitions. Returns the extracted table definitions as plain text. "

        with open(self.sql_file_path, 'r') as file:  
            sql_content = file.read()  
  
        # Use sqlparse to extract table definitions  
        parsed = sqlparse.parse(sql_content)  
        table_definitions = []  
        for statement in parsed:  
            if statement.get_type() == "CREATE":  
                table_definitions.append(str(statement))  
          
        return "\n\n".join(table_definitions)  
  
