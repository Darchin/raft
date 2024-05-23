import json
import os
import sys

def read_from_json(file_path):
    print("[Log]: Checking for corresponding json file in current directory...")
    try:
        if os.path.exists(file_path):
            print("[Log]: Backup file found, checking contents...")
            with open(file_path, 'r') as json_file:
                var_dict = json.load(json_file)
                print(f"[Log]: Variables successfully read from {file_path}")
            return var_dict
        else: 
            print(f"[Log]: Backup file was empty. Initializing to default values...")
            return {}
    
    except Exception as e:
        print(f"[Log]: An error occurred while reading from JSON: {e}")
        sys.exit("[Log]: I/O failure, refusing to join network to prevent inconsistencies.")
    

def write_to_json(file_path, variables):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(variables, json_file, indent=4)
        # print(f"Vital attributes successfully written to {file_path}")
    except Exception as e:
        print(f"[Log]: An error occurred while writing to JSON: {e}")