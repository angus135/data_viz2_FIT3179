import csv
import json

def csv_to_json(csv_filename, json_filename='output.json'):
    """
    Convert CSV file to JSON format suitable for Vega-Lite
    
    Args:
        csv_filename: Path to the input CSV file
        json_filename: Path to the output JSON file (default: 'output.json')
    """
    data = []
    
    # Read the CSV file (tab-separated)
    with open(csv_filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        
        for row in reader:
            # Convert string values to appropriate types
            processed_row = {}
            for key, value in row.items():
                # Try to convert to float for numeric values
                try:
                    # Handle scientific notation (e.g., 8.41861E+11)
                    processed_row[key] = float(value)
                except ValueError:
                    # Keep as string if conversion fails (e.g., state names)
                    processed_row[key] = value
            
            data.append(processed_row)
    
    # Write to JSON file
    with open(json_filename, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=2)
    
    print(f"Successfully converted {csv_filename} to {json_filename}")
    print(f"Total records: {len(data)}")
    
    return data

if __name__ == "__main__":
    # Convert the CSV file
    csv_file = 'top10powerstations.csv'
    json_file = 'top10powerstations.json'
    
    result = csv_to_json(csv_file, json_file)
    
    # Print a sample of the data
    print("\nSample of converted data:")
    print(json.dumps(result[0], indent=2))