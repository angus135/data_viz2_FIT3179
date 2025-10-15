import csv
import json
from datetime import datetime

def convert_csv_to_vegalite_json(csv_file, json_file):
    """
    Convert CSV to JSON format optimized for Vega-Lite.
    """
    data = []
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Convert numeric fields
            record = {
                'Avg_MW': float(row['Avg_MW']),
                'Fuel_Source': row['Fuel_Source'],
                'State': row['State'],
                'AEMO_Station_Name': row['AEMO_Station_Name'],
                'CER_Power_Station_Name': row['CER_Power_Station_Name'],
                'Installed_Capacity_MW': float(row['Installed_Capacity_MW']),
                'Accreditation_Start_Date': row['Accreditation_Start_Date']
            }
            
            # Optionally convert date to ISO format for better Vega-Lite compatibility
            try:
                date_obj = datetime.strptime(row['Accreditation_Start_Date'], '%d/%m/%Y')
                record['Accreditation_Date_ISO'] = date_obj.strftime('%Y-%m-%d')
                record['Accreditation_Year'] = date_obj.year
            except:
                record['Accreditation_Date_ISO'] = None
                record['Accreditation_Year'] = None
            
            data.append(record)
    
    # Write to JSON file
    with open(json_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Converted {len(data)} records from {csv_file} to {json_file}")
    return data

# Example usage
if __name__ == "__main__":
    # Convert the CSV file
    data = convert_csv_to_vegalite_json('project_time_data.csv', 'project_time_data.json')
    
    # Print first record as example
    print("\nFirst record:")
    print(json.dumps(data[0], indent=2))