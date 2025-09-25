import streamlit as st  
import pandas as pd
import autogen  
import os  
import yaml  
import sqlparse  
import pyodbc  
import json  
from simple_ddl_parser import DDLParser  
from dotenv import load_dotenv  
from typing_extensions import Annotated  
from typing import Dict, List  
import re
import matplotlib.pyplot as plt
import pandas as pd

load_dotenv()

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
connection_string = os.getenv("connection_string")

AZURE_OPENAI_API_KEY=AZURE_OPENAI_API_KEY
AZURE_OPENAI_DEPLOYMENT_NAME=AZURE_OPENAI_DEPLOYMENT_NAME
AZURE_OPENAI_ENDPOINT=AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_VERSION=AZURE_OPENAI_API_VERSION
connection_string=connection_string
  

sql_file_path = './sql-scripts/create-tables.sql'  
tables: Dict[str, Dict] = {}  
  
llm_config = {  
    "config_list": [{  
        "model": AZURE_OPENAI_DEPLOYMENT_NAME,  
        "api_key": AZURE_OPENAI_API_KEY,  
        "api_type": "azure",  
        "base_url": AZURE_OPENAI_ENDPOINT,  
        "api_version": AZURE_OPENAI_API_VERSION,  
    }],  
    "temperature": 0.2,  
    "timeout": 300,
}   
  
def load_yaml(path):  
    with open(path, 'r') as f:  
        return yaml.safe_load(f)['instructions']  
  
# Initialize Agents (unchanged)  
agents = {        
    "Metadata_Descriptor": autogen.AssistantAgent(  
        name="Metadata_Descriptor",  
        system_message=load_yaml("./agents/MetadataDescriptor.yaml"),  
        llm_config=llm_config,  
        is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),  
        human_input_mode="NEVER"  
    ),  
    "Tables_Columns_Extractor": autogen.AssistantAgent(  
        name="Tables_Columns_Extractor",  
        system_message=load_yaml("./agents/tables_columns_extractor.yaml"),  
        llm_config=llm_config,  
        is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),  
        human_input_mode="NEVER"  
    ),  
    "SQL_Divide_Conquer": autogen.AssistantAgent(  
        name="SQL_Divide_Conquer",  
        system_message=load_yaml("./agents/DivideConquer.yaml"),  
        llm_config=llm_config,  
        is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),  
        human_input_mode="NEVER"  
    ),  
    "SQL_Query_Executor": autogen.AssistantAgent(  
        name="SQL_Query_Executor",  
        system_message=load_yaml("./agents/sql_query_executor.yaml"),  
        llm_config=llm_config,  
        is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),  
        human_input_mode="NEVER"  
    ),
    "SQL_Explanator": autogen.AssistantAgent(  
    name="SQL_Explanator",  
    system_message=load_yaml("./agents/SQL_Explanator.yaml"),  
    llm_config=llm_config,  
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),  
    human_input_mode="NEVER"  
    
),
    "Chart_Generator": autogen.AssistantAgent(
    name="Chart_Generator",
    system_message=load_yaml("./agents/chart_generator.yaml"),
    llm_config=llm_config,
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
    human_input_mode="NEVER"
),




   
    
}  
  
user_proxy = autogen.ConversableAgent(  
    name="conversation_starter",  
    system_message="You return me the question I give you, one question each line.",  
    llm_config=llm_config,  
    human_input_mode="NEVER"  
)  
  
all_agents_names = list(agents.keys()) + ["User"]  
if 'agent_outputs' not in st.session_state:  
    st.session_state.agent_outputs = {name: "" for name in all_agents_names}  

if "query_result_df" not in st.session_state:
    st.session_state.query_result_df = pd.DataFrame()  # empty by default

if "executed_sql_query" not in st.session_state:
    st.session_state.executed_sql_query = ""

  
# Logging function (unchanged)  
def log_agent_output(recipient, messages, sender, config):  
    last_message = messages[-1]['content']  
    sender_name = sender.name if sender else "System"  
    recipient_name = recipient.name if recipient else "Unknown"  
  
    log = f"Sender: {sender_name}\nRecipient: {recipient_name}\nMessage: {last_message}\n{'-'*50}\n"  
    with open("agent_logs.txt", "a", encoding="utf-8") as f:  
        f.write(log)  
  
    if recipient_name in st.session_state.agent_outputs:  
        st.session_state.agent_outputs[recipient_name] += f"{sender_name}:\n{last_message}\n\n"  
    if sender_name in st.session_state.agent_outputs:  
        st.session_state.agent_outputs[sender_name] += f"{sender_name}:\n{last_message}\n\n"  
  
    return False, None  
  
for agent in list(agents.values()) + [user_proxy]:  
    agent.register_reply([autogen.Agent, None], reply_func=log_agent_output, config={"callback": None})  
  
def connect_to_database() -> str:  
    if not sql_file_path:  
        raise ValueError("SQL file path not defined.")  
    with open(sql_file_path, 'r') as file:  
        sql_content = file.read()  
    parsed = sqlparse.parse(sql_content)  
    table_definitions = [str(s) for s in parsed if s.get_type() == "CREATE"]  
    return "\n\n".join(table_definitions)  
  
import pandas as pd

@user_proxy.register_for_execution()
def execute_sql_on_db(query: Annotated[str, "Execute SQL query"]) -> str:
    query = query.replace(";", "").replace("\n", " ")
    try:
        conn = pyodbc.connect(connection_string)
        df = pd.read_sql(query, conn)
    except Exception as e:
        return f"Error: {e}"
    finally:
        conn.close()
    st.session_state.query_result_df = df
    st.session_state.executed_sql_query = query  # Save the executed query
    return "SQL executed successfully. Result is shown below."

def get_sql_explanator_response() -> str:
    """Return the last message from the SQL_Explanator agent, stripped of extra spaces."""
    agent = agents.get("SQL_Explanator")
    if not agent:
        return "SQL_Explanator agent not found."
    
    last_msg = agent.last_message()
    if not last_msg:
        return "No response generated."
    
    content = last_msg.get("content", "").strip()
    return content if content else "No response generated."

def get_chart_generator_response() -> str:
    """Return the last message from the Chart_Generator agent, stripped of extra spaces."""
    agent = agents.get("Chart_Generator")
    if not agent:
        return "Chart_Generator agent not found."
    
    last_msg = agent.last_message()
    if not last_msg:
        return "No response generated."
    
    content = last_msg.get("content", "").strip()

    # code_blocks = re.findall(r"```python(.*?)```", content, re.DOTALL)
    # cleaned_code = "\n\n".join(block.strip() for block in code_blocks)
    # return cleaned_code
    return content if content else "No response generated."



  

agents["SQL_Query_Executor"].register_for_llm(name='execute_sql_on_db', description="Execute SQL query")(execute_sql_on_db);
  
# Streamlit UI Enhancement  
st.set_page_config(page_title="Multi-Agent SQL Executor", layout="wide")  
  
  

st.markdown("""  
    <style>  
        .main {  
            background-color: #f5f7fa;  
        }  
        .stButton>button {  
            width: 100%;  
            background-color: #4CAF50;  
            color: black; /* Updated text color to black */  
            font-weight: bold; /* Make text bold */  
            font-size: 20px; /* Increase font size by 2 points */  
        }  
        .stTextArea textarea {  
            background-color: #ffffff;  
            color: black; /* Added text color property for the text area */  
            font-weight: bold; /* Make text bold */  
            font-size: 20px; /* Increase font size by 2 points */  
        }  
    </style>  
""", unsafe_allow_html=True)  



# st.title("BI Chatbot")  
  
# Separate panel for query input  

# Header with image side by side
col1, col2 = st.columns([1, 5])  # Adjust ratio
with col1:
    st.image("battery.png", width=100)  # Replace with your logo path
with col2:
    st.header("Do you have a question about Chetak Battery Failures?")

user_input = st.text_area("Please ask a question on Chetak Battery Failures. For example, 'What are the main reasons for battery replacements?' This dataset captures information about Chetak vehicles, their batteries, and related service/complaint events. It includes vehicle identifiers such as VIN, chassis number, SKU codes, brand, and production date; battery lifecycle data covering specifications (capacity, suppliers, configuration), replacement history, charge cycles, and revisions; and tracking attributes like plant codes. It also records service and failure events, including alert date/time, severity, kilometers at failure, defect descriptions, root cause, corrective actions, and customer/dealer details. Some of the data about root causes and corrective actions is sparsely available at this time. The data is currently static till 20th August 2025.", height=100)  
execute_button = st.button("Execute Query")  
  
if execute_button:  
    sql_table_metadata = connect_to_database()
    # print(sql_table_metadata)
    #st.session_state.agent_outputs = {name: "" for name in all_agents_names}  
    user_proxy.initiate_chats([          
        {"recipient": agents["Metadata_Descriptor"], "message": sql_table_metadata, "max_turns": 1, "summary_method": "last_msg"},  
        {"recipient": agents["Tables_Columns_Extractor"], "message": f"Analyze User Query: {user_input}.  Extract relevant tables, columns, and relationships. ", "max_turns": 1, "summary_method": "last_msg"},  
        {"recipient": agents["SQL_Divide_Conquer"], "message": f"Divide user's query into small queries and construct the final SQL to execute on the database. User Query: {user_input}", "max_turns": 1, "summary_method": "last_msg"},  
        {"recipient": agents["SQL_Query_Executor"], "message": "Execute SQL query on the database", "max_turns": 2, "summary_method": "last_msg"},
        {"recipient": agents["SQL_Explanator"], "message": f"Suggest battery failure trends. ", "max_turns": 1, "summary_method": "last_msg"},
        {"recipient": agents["Chart_Generator"], "message": "Suggest Python matplotlib charts to visualize the query result dataframe. Here are the SQL query results:{st.session_state.query_result_df.to_string(index=False)}", "max_turns": 2, "summary_method": "last_msg"},
        
    ])  
    #Here are the SQL query results:{st.session_state.query_result_df.to_string(index=False)}
    # if "query_result_df" in st.session_state:
    #     st.dataframe(st.session_state.query_result_df)
    st.success("✅ Execution completed successfully!")  


  
# # Agent Outputs Display  
# st.header("🖥️ Agent Outputs")  
# for agent_name in all_agents_names:  
#     with st.expander(f"📌 {agent_name} Output", expanded=False):  
#         st.text_area(f"{agent_name} output:", st.session_state.agent_outputs[agent_name], height=200, key=f"{agent_name}_output")  


#st.header("🖥️ Query Output")

if "query_result_df" in st.session_state:
    
    st.subheader("📊 Query Result")
    st.dataframe(st.session_state.query_result_df)

    


    st.subheader("📄 Executed SQL Query")
    st.code(st.session_state.executed_sql_query, language="sql")  # Shows SQL query nicely
else:
 st.write("No result available.")

# insights_output = st.session_state.get("agent_outputs", {}).get("SQL_Explanator", "")
# st.subheader("📈 Data Insights")
# (st.markdown(f"## Response\n{insights_output}"))  
insights_output = get_sql_explanator_response()
st.subheader("📈 Data Description")
st.markdown(insights_output)


st.subheader("📊 Suggested Charts")

if "Chart_Generator" in st.session_state.agent_outputs:
    chart_output = st.session_state.agent_outputs["Chart_Generator"]
    #chart_output=get_chart_generator_response()
    #st.write(chart_output)


    if not st.session_state.query_result_df.empty:
     code_blocks = re.findall(r"```python(.*?)```", chart_output, re.DOTALL)
     if code_blocks:
        code = code_blocks[-1]
        # try:
        #     local_vars = {"df": st.session_state.query_result_df, "plt": plt}
        #     exec(code, {}, local_vars)
        #     st.pyplot(plt.gcf())
        #     plt.clf()
        # except Exception as e:
        #     st.error(f"⚠️ Error rendering chart: {e}")
        try:
                # Fresh locals each time
              local_vars = {
                  
                "df": st.session_state.query_result_df.copy(),  # fresh copy of df
                "plt": plt
                }
              exec(code, {}, local_vars)
 
              st.pyplot(plt.gcf())
              plt.close()  # close figure so it won’t leak into next chart
        except Exception as e:
                st.error(f"⚠️ Error rendering chart: {e}")
    else:
     st.warning("No query results available to generate charts.")


# st.subheader("📊 Suggested Charts")
 
# if "Chart_Generator" in st.session_state.agent_outputs:
#     chart_output = st.session_state.agent_outputs["Chart_Generator"]
 
#     if not st.session_state.query_result_df.empty:
#         # Extract all code blocks and remove duplicates
#         code_blocks = list(dict.fromkeys(
#             re.findall(r"```python(.*?)```", chart_output, re.DOTALL)
#         ))
 
#         for code in code_blocks:
#             try:
#                 # Fresh locals each time
#                 local_vars = {
#                     "df": st.session_state.query_result_df.copy(),  # fresh copy of df
#                     "plt": plt
#                 }
#                 exec(code, {}, local_vars)
 
#                 st.pyplot(plt.gcf())
#                 plt.close()  # close figure so it won’t leak into next chart
#             except Exception as e:
#                 st.error(f"⚠️ Error rendering chart: {e}")
#     else:
#         st.warning("No query results available to generate charts.")



# else:
#     st.write("No result available.")

