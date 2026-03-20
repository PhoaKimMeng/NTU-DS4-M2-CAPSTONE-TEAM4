import os
import argparse
import pandas as pd

def upload_to_bigquery(input_dir, project_id, dataset_id, replace=True):
    """
    Reads processed CSV files from the input directory and uploads them to Google BigQuery.
    """
    if not os.path.exists(input_dir):
        print(f"Error: Directory '{input_dir}' does not exist.")
        return

    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in '{input_dir}'.")
        return

    print(f"Found {len(csv_files)} CSV files. Starting upload to BigQuery (Project: {project_id}, Dataset: {dataset_id})")

    for file_name in csv_files:
        table_name = file_name.replace('.csv', '')
        file_path = os.path.join(input_dir, file_name)
        
        print(f"\nProcessing '{file_name}' -> Table '{dataset_id}.{table_name}'")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Determine if we replace or append
            if_exists_behavior = 'replace' if replace else 'append'
            
            # Upload to BigQuery
            print(f"Uploading {len(df)} rows to {dataset_id}.{table_name}...")
            df.to_gbq(
                destination_table=f"{dataset_id}.{table_name}",
                project_id=project_id,
                if_exists=if_exists_behavior
            )
            print(f"Successfully uploaded {table_name}")
            
        except Exception as e:
            print(f"Failed to upload '{table_name}': {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export processed CSV data to Google BigQuery.")
    parser.add_argument("--project-id", type=str, required=True, help="Your Google Cloud Project ID.")
    parser.add_argument("--dataset-id", type=str, required=True, help="Your BigQuery Dataset ID (e.g., my_dataset).")
    parser.add_argument("--input-dir", type=str, default="processed_data", help="Directory containing the processed CSV files.")
    parser.add_argument("--append", action="store_true", help="Append to existing tables instead of replacing them.")
    
    args = parser.parse_args()
    
    # Resolve the input directory path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_directory = os.path.join(base_dir, args.input_dir)
    
    # If the user passed an absolute path, use it directly instead
    if os.path.isabs(args.input_dir):
        input_directory = args.input_dir
        
    upload_to_bigquery(
        input_dir=input_directory,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        replace=not args.append
    )
