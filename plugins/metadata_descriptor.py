from semantic_kernel import Kernel  

from semantic_kernel.functions import kernel_function
from typing import Annotated

import pyodbc
import sqlparse  
from semantic_kernel import Kernel  

from semantic_kernel.functions import kernel_function
from simple_ddl_parser import DDLParser
import pyodbc
from typing import Dict, List
import json


class MetadataDescriptor:
    """
    A class to describe the metadata of a SQL file using a language model.        
    """

    
    def __init__(self):  
        self.tables: Dict[str, Dict] = {}
        
  
  
      
    @kernel_function(name="describe_tables_and_columns",
                     description= "Extracts table definitions and Uses the LLM to describe the table definition.")    
    def parse_sql_describe_table(self, sql_table_definition: Annotated[str,"The sql table definitions"]) -> Annotated[str, "The description of the table in human readable format"]:
        """  
        Uses the LLM to describe the given table definition.  
        """ 
        response = []
        parser = DDLParser(sql_table_definition)

        parsed_schema = parser.run()

        for table_def in parsed_schema:
            table_name = table_def['table_name']
            columns = {col['name']: {"type": col['type'], "examples": []} for col in table_def['columns']}
            primary_keys = table_def.get('primary_key', [])
            foreign_keys = table_def.get('foreign_keys', [])

            self.tables[table_name] = {
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys
            }
           
        
        print(json.dumps(self.tables, indent=4))
        return json.dumps(self.tables, indent=4)  
  