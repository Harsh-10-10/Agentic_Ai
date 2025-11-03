#### **0. Initial Interaction**
- **C.T.:** Welcome! Please upload your data file to start.
- **User:** [Uploads file]
 

 
#### 1. **Task Selection**
- **C.T.:** What task should I do for you today?
  - Options:
    - Profile the data (DP)
    - Validate schema (SV)
    - Both
    - Help (list available operations)
  - **Edge Case:** If user input is ambiguous or unrecognized, ask for clarification.
 

 
#### **2A. Data Profiling Route (DP)**
 
##### a. File Conversion & Processing
- **DP:** File received. Proceeding to convert to DataFrame and preprocess.
  - **Edge Case:** If file conversion fails, notify user and request a supported format.
 
##### b. Configure Sample Size
- **DP:** How many samples/rows for analysis? (Default = 5)
  - **Edge Case:** If input is invalid (non-integer, negative, too large), ask for valid value or use default.
 
##### c. Output Format Choice
- **DP:** Output in JSON or Markdown? (Default = Markdown)
  - **Edge Case:** If unrecognized format, prompt user to choose again.
 
##### d. Display Results
- **DP:** [Display Data Profile in the selected format]
- **C.T.:** Any follow-up questions or another task for me?
  

 
#### **2B.  Schema Validation Route (SV)**
 
##### a. File Conversion & Initial Processing
- **SV:** File received. Proceeding to convert to DataFrame.
  - **Edge Case:** Handle file conversion errors as above.
 
##### b. Table Name Entry
- **SV:** Please enter table name (e.g., "customer_orders").
  - **Edge Case:** If invalid table name or no default available, request explicit table name or display DataProfiler result in MD.
 
##### c. Output Format Choice
- **SV:** Output in JSON or Markdown? (Default = Markdown)
  - **Edge Case:** If unrecognized format, prompt user to choose again.
 
##### d. Display Results
- **SV:** [Present schema in selected format]
- **SV:** Any follow-up questions or more tasks?
  - **Edge Case:** If schema is missing or invalid, explain to user and offer troubleshooting steps.
 

 
#### **2C. Combined Task**
- **C.T.:** Execute Data Profiling and Schema Validation sequentially as per above steps. Relay outputs and follow-up prompts for each.
 
***
 

### Example Interactions
 
#### Profile the Data
| Step      | Tool | Dialog |
|-----------|------|--------|
| 1         | C.T. | Please upload your data file. |
| 2         | User | [uploads file] |
| 3         | C.T. | What should I do? (Profile/Validate/Help) |
| 4         | User | Profile the data.  |
| 5         | DP   | How many samples? (Default = 10) |
| 6         | User | 10 rows |
| 7         | DP   | Output in JSON or Markdown? (Default = Markdown) |
| 8         | User | JSON |
| 9         | DP   | [Shows samples in JSON] |
| 10        | DP   | Any follow-up questions or another task? |
 
#### Validate the Schema
| Step      | Tool | Dialog |
|-----------|------|--------|
| 1         | C.T. | Please upload your data file. |
| 2         | User | [uploads file] |
| 3         | C.T. | What should I do? (Profile/Validate/Help) |
| 4         | User | Validate schema. |
| 5         | SV   | Enter table name (default = "customer_orders") |
| 6         | User | customer_orders |
| 7         | SV   | Output in JSON or Markdown? (Default = Markdown) |
| 8         | User | [enters] |
| 9         | SV   | [Shows schema in Markdown] |
| 10        | SV   | Any follow-up questions or more tasks for me? |
 
***
 
### General Edge Case Scenarios Table
 
| Condition                  | Handling Step                                                   |
|----------------------------|-----------------------------------------------------------------|
| Unsupported file format    | Prompt for supported format upload                              |
| Empty data                 | Notify, suggest troubleshooting or alternate upload             |
| Invalid sample size        | Request correction or enforce default                          |
| No table detected          | List options or prompt for explicit entry                      |
| Unrecognized format type   | Clarify formats, enforce default                               |
| Tool error (internal)      | Report error, suggest alternative actions                      |
 
***

Here,
|Abbrevation|       Tools        |
|-----------|--------------------|
|   C.T.    |Conversation Tool   |
|   DP      |DataProfilerAgent   |
|   SV      |SchemaValidatorAgent|