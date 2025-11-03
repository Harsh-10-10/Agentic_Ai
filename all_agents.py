import os
import autogen
from typing import Annotated  # <-- CHANGED: Added Annotated
import json  # <-- CHANGED: Added json for the run_multi_sheet_validation tool
from config import config_list
from data_connector import read_data_file
from DataProfilerAgent_end_to_end import *
# Import the validation module if it's in a separate file (as implied by your code)
# import schema_validation_module as validation_module # <-- Uncomment this if needed

# --- 1. Configuration ---
from dotenv import load_dotenv
load_dotenv()

AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

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

#
# >>>>> START OF CHANGES (SECTION 2) <<<<<
#
# The profiler function is redesigned to be a valid autogen tool.
# 1. It accepts a 'file_path' (str) instead of a 'df' (object).
# 2. All arguments are properly type-annotated using 'Annotated'.
# 3. It loads the data *inside* the function.
# 4. It returns a string, as required by autogen tools.
#
def profiler(
    file_path: Annotated[str, "The path to the data file (e.g., 'customer_orders.csv')"],
    sample_size: Annotated[int, "The number of samples to use for profiling"] = 5
) -> str:
    """
    Loads a data file from a file path, profiles it, and returns the profiling report as a string.
    """
    print("\n" + "="*30)
    print(f"... EXECUTING: profiler('{file_path}, {sample_size})...")
    
    input_filename = "customer_orders_wrong_data.csv"

    try:
        if not os.path.exists(input_filename):
            logging.error(f"Input file not found: {input_filename}. Please create it or change the filename.")
        else:
            profiler_agent = DataProfilerAgent(
                api_version=AZURE_OPENAI_API_VERSION,
                endpoint=AZURE_OPENAI_ENDPOINT,
                api_key=AZURE_OPENAI_KEY,
                deployment=DEPLOYMENT_NAME
            )
            try:
                df = read_data_file(input_filename)
            except Exception as e:
                logging.error(f"Failed to read or process CSV file: {e}")
            else:
                report_str = profiler_agent.profile(df, input_filename,take_sample_size=7)
            return report_str
        
    except Exception as e:
        error_str = f'{{"error": "Failed to run profiler: {e}"}}'
        print(f"... ERROR: {e}")
        print("="*30 + "\n")
        return error_str
#
# >>>>> END OF CHANGES (SECTION 2) <<<<<
#

def run_multi_sheet_validation(
    file_path: Annotated[str, "The file path to the CSV or Excel file"],
    table_name: Annotated[str, "The target database table name"]
) -> Annotated[str, "The JSON string result of the schema validation"]:
    """
    Runs the full multi-sheet schema validation process on a given file
    against a specific database table.
    """
    print("\n" + "="*30)
    print(f"... EXECUTING: run_multi_sheet_validation('{file_path}', '{table_name}')...")
    
    try:
        # Call the main function from your refactored module
        # This assumes 'validation_module' is imported correctly
        final_report_json = validation_module.run_validation(file_path, table_name)
        
        result = json.dumps(final_report_json)
        
        print(f"... RESULT: {result[:200]}...") # Print a snippet
        print("="*30 + "\n")
        return result
        
    except Exception as e:
        print(f"... ERROR: {e}")
        print("="*30 + "\n")
        return f'{{"error": "Failed to run validation: {e}"}}'

# --- 3. Agent Definitions ---

user_proxy = autogen.UserProxyAgent(
    name="UserProxy",
    human_input_mode="TERMINATE",
    max_consecutive_auto_reply=10,
    is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
    code_execution_config=False,
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
                You will be given a file path and a report filename.
                Do not write any other text.
                Just call the tool, get the result, and reply with the result and TERMINATE."""
)

schema_validator_agent = autogen.ConversableAgent(
    name="SchemaValidatorAgent",
    llm_config=llm_config,
    system_message="""You are a specialist schema validator agent.
                Your *only* job is to call the `run_multi_sheet_validation` tool when asked to validate.
                Do not write any other text.
                Just call the tool, get the result, and reply with the result and TERMINATE."""
)

# --- 4. Tool Registration ---

autogen.register_function(
    profiler,
    caller=data_profiler_agent,
    executor=user_proxy,
    description="Run the data profiler on a given file path.",
)

'''autogen.register_function(
    run_multi_sheet_validation,
    caller=schema_validator_agent,
    executor=user_proxy,
    description="Run the multi-sheet schema validator",
)'''

# --- 5. Group Chat and Orchestrator (Manager) Setup ---

agents = [user_proxy, conversation_tool, data_profiler_agent, schema_validator_agent]

group_chat = autogen.GroupChat(
    agents=agents,
    messages=[],
    max_round=6,
)

orchestrator = autogen.GroupChatManager(
    name="Orchestrator",
    groupchat=group_chat,
    llm_config=llm_config,
    system_message="""You are the Orchestrator.
            Your job is to read the user's request and select the correct agent.
            - If the request is for profiling, select 'DataProfilerAgent'.
            - If the request is for validation, select 'SchemaValidatorAgent'.
            - If the request is a general question, select 'ConversationTool'.
            - 'UserProxy' executes tools.
            Let the selected agent work until they finish (say TERMINATE).
    """
)

# --- 6. Run the Chat ---

print("="*50)
print("🚀 STARTING CHAT 1: DATA PROFILING")
print("="*50)

input_file = "customer_orders_wrong_data.csv"

#
# >>>>> START OF CHANGES (SECTION 6) <<<<<
#
# 1. Removed the 'df = read_data_file(...)' line. The tool loads the data now.
# 2. Changed the message to be a natural language request
#    that provides the *file path* and the *report filename*.
#
user_proxy.initiate_chat(
    orchestrator,
    message=f"Please profile the dataset from the file '{input_file}'."
)
#
# >>>>> END OF CHANGES (SECTION 6) <<<<<
#

"""print("\n" * 2)
print("="*50)
print("🚀 STARTING CHAT 2: SCHEMA VALIDATION")
print("="*50)

# Start a new chat for a different task
user_proxy.initiate_chat(
    orchestrator,
    message="Now, please run the schema validation on all the sheets."
)

print("\n" * 2)
print("="*50)
print("🚀 STARTING CHAT 3: CONVERSATION")
print("="*50)

# Start a new chat for a conversational task
user_proxy.initiate_chat(
    orchestrator,
    message="What is the purpose of schema validation?"
)"""