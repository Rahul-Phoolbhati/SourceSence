#!/usr/bin/env python3
"""
Script to save existing metadata from the object store to a JSON file in the main directory.
"""

import json
import os
import time
from typing import Any, Dict

def save_metadata_to_json(workflow_id: str, run_id: str, output_dir: str = "./local/dapr/objectstore/artifacts") -> None:
    """
    Collect and save extracted metadata to a JSON file in the main directory.
    
    Args:
        workflow_id: The workflow ID
        run_id: The workflow run ID
        output_dir: Directory where artifacts are stored
    """
    try:
        # Path to the transformed data
        transformed_dir = f"{output_dir}/apps/default/workflows/{workflow_id}/{run_id}/transformed"
        
        metadata = {
            "workflow_info": {
                "workflow_id": workflow_id,
                "run_id": run_id,
                "extraction_timestamp": time.time()
            },
            "extracted_metadata": {}
        }
        
        # Collect metadata from different asset types
        asset_types = ["table", "database", "schema", "column"]
        
        for asset_type in asset_types:
            asset_dir = f"{transformed_dir}/{asset_type}"
            if os.path.exists(asset_dir):
                file = f"chunk-0-part1.json"
                # print
                asset_data = []
                
          
                file_path = os.path.join(asset_dir, file)
                try:
                    with open(file_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue  # Skip empty lines
                            try:
                                obj = json.loads(line)
                                asset_data.append(obj)
                            except json.JSONDecodeError as je:
                                print(f"Warning: Skipping invalid JSON line in {file_path}: {je}")
                except Exception as e:
                    print(f"Warning: Error reading {file_path}: {e}")
                
                metadata["extracted_metadata"][asset_type] = asset_data
                print(f"Collected {len(asset_data)} {asset_type} records")
        
        # Save to JSON file in main directory
        output_file = f"extracted_metadata_{workflow_id}_{int(time.time())}.json"
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        print(f"Metadata saved to {output_file}")
        print(f"Total records: {sum(len(records) for records in metadata['extracted_metadata'].values())}")
        
    except Exception as e:
        print(f"Error saving metadata to JSON: {e}")

def find_latest_workflow():
    """Find the most recent workflow directory."""
    artifacts_dir = "./local/dapr/objectstore/artifacts/apps/default/workflows"
    if not os.path.exists(artifacts_dir):
        print("No workflows found")
        return None, None
    
    workflow_dirs = [d for d in os.listdir(artifacts_dir) if os.path.isdir(os.path.join(artifacts_dir, d))]
    if not workflow_dirs:
        print("No workflow directories found")
        return None, None
    
    # Get the most recent workflow
    latest_workflow = max(workflow_dirs)
    workflow_path = os.path.join(artifacts_dir, latest_workflow)
    
    # Get the most recent run
    run_dirs = [d for d in os.listdir(workflow_path) if os.path.isdir(os.path.join(workflow_path, d))]
    if not run_dirs:
        print("No run directories found")
        return None, None
    
    latest_run = max(run_dirs)
    return latest_workflow, latest_run

if __name__ == "__main__":
    print("Looking for latest workflow...")
    workflow_id, run_id = find_latest_workflow()
    
    if workflow_id and run_id:
        print(f"Found workflow: {workflow_id}, run: {run_id}")
        save_metadata_to_json(workflow_id, run_id)
    else:
        print("No workflow found to process")
