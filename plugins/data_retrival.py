
from semantic_kernel.functions import kernel_function
import os
import asyncio


from azure.ai.projects.models import FileSearchTool, OpenAIFile, VectorStore
from azure.identity.aio import DefaultAzureCredential
from azure.ai.projects import AIProjectClient

from semantic_kernel.agents import   AzureAIAgent, AzureAIAgentSettings, AzureAIAgentThread
from semantic_kernel.contents import AuthorRole
from azure.ai.projects.models import (
    AgentEventHandler,
    RunStep,
    RunStepDeltaChunk,
    ThreadMessage,
    ThreadRun,
    MessageDeltaChunk,
    BingGroundingTool,
    FilePurpose,
    FileSearchTool,
    FunctionTool,
    ToolSet
)

from dotenv import load_dotenv
load_dotenv()

import pyodbc


class DataRetrival:
    """
    Description: Get the documents from the knowledge base.
    """
    def __init__(self, connection_string) -> None:
        pass
    
    
    @staticmethod
    async def load_vector_store(project_client):
        FOLDER_NAME = "D:/MS/Git/NL2SQL-Agnetic/resources"
        VECTOR_STORE_NAME = "hr-policy-vector-store"
        all_vector_stores = await project_client.agents.list_vector_stores()
        existing_vector_store = next(
            (store for store in all_vector_stores.data if store.name == VECTOR_STORE_NAME),
            None
        )
        vector_store_id = None
        if existing_vector_store:
            vector_store_id = existing_vector_store.id
            print(f"reusing vector store > {existing_vector_store.name} (id: {existing_vector_store.id})")
        else:
            # If you have local docs to upload
            import os
            if os.path.isdir(FOLDER_NAME):
                file_ids = []
                for file_name in os.listdir(FOLDER_NAME):
                    file_path = os.path.join(FOLDER_NAME, file_name)
                    if os.path.isfile(file_path):
                        print(f"uploading > {file_name}")
                        uploaded_file =  await project_client.agents.upload_file_and_poll(
                            file_path=file_path,
                            purpose=FilePurpose.AGENTS
                        )
                        file_ids.append(uploaded_file.id)

                if file_ids:
                    print(f"creating vector store > from {len(file_ids)} files.")
                    vector_store = await  project_client.agents.create_vector_store_and_poll(
                        file_ids=file_ids,
                        name=VECTOR_STORE_NAME
                    )
                    vector_store_id = vector_store.id
                    print(f"created > {vector_store.name} (id: {vector_store_id})")

        file_search_tool = None
        if vector_store_id:
            file_search_tool = FileSearchTool(vector_store_ids=[vector_store_id])

        return file_search_tool 
    

    @kernel_function(name="search_files", description="Search files in the vector store")
    async def search_files(
        self,         
        query: str        
    ):
        """
        Description: Search the vector store for the given query.
        Arguments:
            query (str): The query to search for.
        Returns:
            str: The search results.
        """
        # Use the Azure AI Project client to search the vector store
        project_client = AIProjectClient(credential=DefaultAzureCredential())
        file_search_tool = await self.load_vector_store(project_client)
        
        if file_search_tool is None:
            return "No vector store found."

        # Perform the search using the file search tool
        search_results = await file_search_tool.search(query=query)
        
        return search_results
        
        
       
    
                     
    

        