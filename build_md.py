import json
 
def create_validation_markdown(data: dict) -> str:
    """
    Converts a data validation JSON (as a dictionary) into a formatted Markdown string.
 
    This function is designed to be robust and will not fail if keys are missing,
    using .get() to provide default values.
 
    Args:
        data: A dictionary loaded from the data validation JSON.
 
    Returns:
        A string containing the formatted Markdown report.
    """
    md_parts = []
   
    # Helper to safely format lists of items
    def format_list(items_list, empty_msg="None"):
        if not items_list:
            return f"- {empty_msg}"
        return "\n".join(f"- `{item}`" for item in items_list)
 
    # --- Title and Overall Summary ---
    file_name = data.get('User_file_name', 'N/A')
    md_parts.append(f"# Data Validation Report for '{file_name}'")
   
    summary = data.get('validation_summary', {})
    status = summary.get('status', 'N/A')
    md_parts.append(f"\n## Validation Status: **{status}**")
   
    overall_analysis = data.get('overall_analysis', {})
    md_parts.append(f"\n**Overall Analysis:** {overall_analysis.get('summary', 'N/A')}")
 
    # --- Summary Table ---
    md_parts.append("\n### At a Glance")
    md_parts.append("| Metric | Value |")
    md_parts.append("| :--- | :--- |")
    md_parts.append(f"| Target Table (Inferred) | `{data.get('inferred_target_table', 'N/A')}` |")
    md_parts.append(f"| High Severity Issues | {summary.get('high_severity_issues', 0)} |")
    md_parts.append(f"| Medium Severity Issues | {summary.get('medium_severity_issues', 0)} |")
    md_parts.append(f"| Low Severity Issues | {summary.get('low_severity_issues', 0)} |")
    md_parts.append(f"| Total Rows Checked | {data.get('total_rows_checked', 'N/A')} |")
    md_parts.append(f"| Processed At | {data.get('Processed_at', 'N/A')} |")
 
    # --- Schema Mismatch ---
    md_parts.append("\n## 1. Schema Mismatch Analysis")
    schema = data.get('schema_mismatch', {})
    if not schema:
        md_parts.append("No schema mismatch data found.")
    else:
        md_parts.append(f"**Analysis:** {schema.get('analysis', {}).get('context', 'N/A')}")
       
        md_parts.append("\n#### Columns Missing from File (Required by Table):")
        md_parts.append(format_list(schema.get('columns_missing_from_file'), "None"))
       
        md_parts.append("\n#### Extra Columns Found in File (Not in Table):")
        md_parts.append(format_list(schema.get('columns_extra_in_file'), "None"))
 
        md_parts.append("\n#### Suggested Naming Mappings:")
        mappings = schema.get('naming_mismatches', {})
        if not mappings:
            md_parts.append("- None")
        else:
            for file_col, db_col in mappings.items():
                md_parts.append(f"- Map `{file_col}` (file) to `{db_col}` (table)")
       
        md_parts.append("\n#### Recommendations:")
        md_parts.append(format_list(schema.get('analysis', {}).get('recommendation', []), "No recommendations."))
 
    # --- Data Type Mismatch ---
    md_parts.append("\n## 2. Data Type Violations")
    type_mismatches = data.get('data_type_mismatch', [])
    if not type_mismatches:
        md_parts.append("No data type mismatches found.")
    else:
        for issue in type_mismatches:
            md_parts.append(f"\n- **Column: `{issue.get('column')}`**")
            md_parts.append(f"  - **Severity:** {issue.get('severity', 'N/A').title()}")
            md_parts.append(f"  - **Expected Type:** `{issue.get('expected_db_type')}`")
            md_parts.append(f"  - **Found Type:** `{issue.get('found_file_type')}`")
            md_parts.append(f"  - **Invalid Samples:** `{issue.get('sample_invalid_values', [])}`")
            md_parts.append(f"  - **Suggestion:** {issue.get('normalization_suggestion', 'N/A')}")
            md_parts.append(f"  - **Cleaning Code:** `{issue.get('suggested_cleaning_code', 'N/A')}`")
 
    # --- Data Quality Violations ---
    md_parts.append("\n## 3. Data Quality Violations")
    dq_violations = data.get('data_quality_violations', [])
    if not dq_violations:
        md_parts.append("No data quality violations found.")
    else:
        for issue in dq_violations:
            md_parts.append(f"\n- **Column: `{issue.get('column')}`**")
            md_parts.append(f"  - **Check:** `{issue.get('check')}`")
            md_parts.append(f"  - **Severity:** {issue.get('severity', 'N/A').title()}")
            md_parts.append(f"  - **Count:** {issue.get('count', 'N/A')}")
            md_parts.append(f"  - **Details:** {issue.get('details', 'N/A')}")
 
    # --- Root Cause Analysis ---
    md_parts.append("\n## 4. Root Cause Analysis")
    rca = data.get('root_cause_analysis', {})
    if not rca:
        md_parts.append("No root cause analysis found.")
    else:
        md_parts.append(f"**Primary Cause:** {rca.get('primary_cause', 'N/A')}")
        md_parts.append("\n**Secondary Causes:**")
        md_parts.append(format_list(rca.get('secondary_causes', []), "None"))
        md_parts.append("\n**Recommendations:**")
        md_parts.append(format_list(rca.get('recommendations', []), "None"))
 
    # --- Load Strategy ---
    md_parts.append("\n## 5. Suggested Load Strategy")
    strategy = data.get('append_upsert_suggestion', {})
    if not strategy:
        md_parts.append("No load strategy analysis found.")
    else:
        md_parts.append(f"- **Strategy:** `{strategy.get('strategy', 'N/A').upper()}`")
        md_parts.append(f"- **Reasoning:** {strategy.get('reasoning', 'N/A')}")
        md_parts.append(f"- **Recommendation:** {strategy.get('recommendation', 'N/A')}")
 
    # --- Schema Drift ---
    md_parts.append("\n## 6. Schema Drift")
    drift = data.get('schema_drift', {})
    if not drift:
        md_parts.append("No schema drift analysis found.")
    else:
        md_parts.append(f"**Analysis Summary:** {drift.get('analysis_summary', 'N/A')}")
        md_parts.append("\n**New Columns Detected:**")
        md_parts.append(format_list(drift.get('new_columns_detected', []), "None"))
        md_parts.append("\n**Removed Columns Detected:**")
        md_parts.append(format_list(drift.get('removed_columns_detected', []), "None"))
 
    # --- Dynamic Validation Rules ---
    md_parts.append("\n## 7. Inferred Validation Rules")
    rules = data.get('dynamic_validation_rules', [])
    if not rules:
        md_parts.append("No dynamic validation rules were inferred.")
    else:
        md_parts.append("| Column | Rule Type | Details | Inferred From |")
        md_parts.append("| :--- | :--- | :--- | :--- |")
        for rule in rules:
            col = f"`{rule.get('column', 'N/A')}`"
            rule_type = f"`{rule.get('rule_type', 'N/A')}`"
            details = rule.get('rule_details', 'N/A')
            samples = f"`{rule.get('inferred_from_samples', [])}`"
            md_parts.append(f"| {col} | {rule_type} | {details} | {samples} |")
 
    # Join all parts with newlines and return
    return "\n".join(md_parts)
 
# --- Example of how to use the function ---
if __name__ == "__main__":
   
    # This is the new file you uploaded.
    # Make sure this file is in the same directory as the Python script.
    json_file_path = 'validation_report.json'
    output_markdown_file = 'data_validation_report.md'
 
    try:
        # Open and load the JSON data
        with open(json_file_path, 'r', encoding='utf-8') as f:
            validation_data = json.load(f)
           
        # Call the function to generate markdown
        print(f"Generating markdown from '{json_file_path}'...")
        markdown_output = create_validation_markdown(validation_data)
       
        # Print to console (optional)
        print("\n--- MARKDOWN PREVIEW ---")
        print(markdown_output)
        print("------------------------\n")
 
        # Save the markdown to a new file
        with open(output_markdown_file, 'w', encoding='utf-8') as md_file:
            md_file.write(markdown_output)
           
        print(f"Successfully generated and saved report to '{output_markdown_file}'")
 
    except FileNotFoundError:
        print(f"Error: The file '{json_file_path}' was not found.")
        print("Please make sure it's in the same folder as this script.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file_path}'.")
        print("The file might be corrupted or not valid JSON.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
 