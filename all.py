import os
import autogen
import json 
from typing import Annotated, Optional
# --- 1. IMPORT YOUR REAL CODE ---
import validation_module
from dotenv import load_dotenv

# --- Load Config ---
load_dotenv()

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

DEPLOYMENT_NAME = AZURE_OPENAI_DEPLOYMENT

try:
    from config import config_list
except ImportError:
    print("Warning: 'config.py' not found.")
    config_list = [
        {
            "model": AZURE_OPENAI_DEPLOYMENT,
            "api_key": AZURE_OPENAI_KEY,
            "base_url": AZURE_OPENAI_ENDPOINT,
            "api_type": "azure",
            "api_version": AZURE_OPENAI_API_VERSION,
        }
    ]

llm_config = {
    "config_list": config_list,
    "cache_seed": 42,
    "timeout": 120,
}

# --- 2. Tool Definitions ---

def profiler(
    file_path: Annotated[str, "The file path to the CSV or Excel file"]
) -> Annotated[str, "The JSON string result of the data profiling"]:
    """
    Runs a data profiling process on the available data.
    (This is a placeholder for your friend's code)
    """
    print("\n" + "="*30)
    print(f"... EXECUTING: profiler('{file_path}')...")
    # Simulate work and return a result
    result = f'{{"file": "{file_path}", "columns": 10, "rows": 5000, "nulls_found": true}}'
    print(f"... RESULT: {result}")
    print("="*30 + "\n")
    return result

# This is your REAL, refactored tool
def run_multi_sheet_validation(
    file_path: Annotated[str, "The file path to the CSV or Excel file"], 
    table_name: Annotated[Optional[str], "The target DB table. Use 'None' to check all."] = None
) -> Annotated[str, "The JSON string result of the schema validation"]:
    """
    Runs the full multi-sheet schema validation process on a given file
    against a specific database table.
    """
    print("\n" + "="*30)
    print(f"... EXECUTING: run_multi_sheet_validation('{file_path}', '{table_name}')...")
    
    try:
        # Call the main function from your refactored module
        final_report_dict = validation_module.run_multi_sheet_validation(
            file_path=file_path,
            db_url="sqlite:///database/sample_data.db", # Hardcoded for the POC
            user_provided_table_name=table_name
        )
        
        # 'autogen' tools MUST return a single string
        result_string = json.dumps(final_report_dict) 
        
        print(f"... RESULT: (Success, {len(result_string)} bytes generated)")
        print("="*30 + "\n")
        return result_string
        
    except Exception as e:
        error_message = f'{{"error": "Failed to run validation: {e}"}}'
        print(f"... ERROR: {error_message}")
        print("="*30 + "\n")
        return error_message
# --- END TOOL DEFINITION ---


# --- 3. Agent Definitions ---

# --- THIS WAS THE MISSING PART ---
user_proxy = autogen.UserProxyAgent(
   name="UserProxy",
   human_input_mode="TERMINATE", 
   max_consecutive_auto_reply=10,
   is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
   code_execution_config=False,  # We are using tool registration, not code execution
   system_message="You are the user. You initiate the request and execute tool calls. Reply TERMINATE when the task is complete."
)

conversation_tool = autogen.ConversableAgent(
   name="ConversationTool",
   llm_config=llm_config,
   system_message="""You are a helpful conversational AI for data analysis.
                   You chat with the user in natural language to clarify data-related questions.
                   You do not have any tools.
                   When your task is done, provide the answer and then say TERMINATE."""
)

data_profiler_agent = autogen.ConversableAgent(
   name="DataProfilerAgent",
   llm_config=llm_config,
   system_message="""You are a specialist data profiler agent.
                   Your *only* job is to call the `profiler` tool when asked to profile data.
                   You must extract the 'file_path' from the request.
                   Do not write any other text.
                   Just call the tool, get the result, and reply with the result and TERMINATE."""
)
# --- END OF MISSING PART ---

# 4. SchemaValidatorAgent (Worker)
schema_validator_agent = autogen.ConversableAgent(
    name="SchemaValidatorAgent",
    llm_config=llm_config,
    system_message="""You are a specialist schema validator agent.
            Your *only* job is to call the `run_multi_sheet_validation` tool when asked to validate.
            You must extract the 'file_path' and 'table_name' from the request.
            Do not write any other text.
            Just call the tool, get the result, and reply with the result and TERMINATE."""
)


# --- 4. Tool Registration ---
autogen.register_function(
    profiler,
    caller=data_profiler_agent,
    executor=user_proxy,
    description="Run the data profiler",
)

autogen.register_function(
    run_multi_sheet_validation,
    caller=schema_validator_agent, 
    executor=user_proxy, 
    description="Run the multi-sheet schema validator",
)

# --- 5. Group Chat and Orchestrator Setup ---
agents = [user_proxy, conversation_tool, data_profiler_agent, schema_validator_agent]
group_chat = autogen.GroupChat(
    agents=agents,
    messages=[],
    max_round=10,
)

orchestrator = autogen.GroupChatManager(
    name="Orchestrator",
    groupchat=group_chat,
    llm_config=llm_config,
    system_message="""You are the Orchestrator. Your job is to manage the workflow.

    **Workflow:**
    1.  **New Request:** If the last message is from the `UserProxy` and it's a new task (e.g., "Please run..."), you MUST select the correct specialist agent (`DataProfilerAgent` or `SchemaValidatorAgent`).
    2.  **Tool Result:** If the last message is from the `UserProxy` and it contains `"***** Response from calling tool *****"`, this is a tool result. Your **only job** is to select the `ConversationTool` to summarize this JSON result for the user.
    3.  **General Question:** If the user asks a general question, select `ConversationTool`.
    4.  **Task Complete:** If the `ConversationTool` has just provided its final summary, your job is complete. Reply with the word TERMINATE.

    **Agent Roles:**
    - `UserProxy`: Runs tools.
    - `DataProfilerAgent`, `SchemaValidatorAgent`: Specialists who request tools.
    - `ConversationTool`: Summarizes JSON results and talks to the user.

    Do not get stuck in a loop. Follow the workflow.
    """
)

# --- 6. Run the Chat ---
# Now your message MUST provide the arguments
print("="*50)
print("🚀 STARTING CHAT: SCHEMA VALIDATION")
print("="*50)

# Use a real file name from your project
user_proxy.initiate_chat(
    orchestrator,
    message="Please run the schema validation on the file 'new_order.csv' and compare it to the 'customer_orders' table."
)

print("\n" * 2)
print("="*50)
print("🚀 STARTING CHAT 2: DATA PROFILING")
print("="*50)

user_proxy.initiate_chat(
    orchestrator,
    message="Please profile the file 'new_order.csv'."
)