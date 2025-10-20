import pandas as pd
from rapidfuzz import fuzz, process
import json

# Read the three CSV files
print("Reading CSV files...")
aemo_data = pd.read_csv('aemo_2018_produced_mwh.csv', encoding='utf-8')
# Try different encodings for the CER file
try:
    cer_data = pd.read_csv('historical-accredited-power-stations-and-projects-0.csv', encoding='utf-8')
except UnicodeDecodeError:
    print("UTF-8 failed, trying latin-1 encoding...")
    try:
        cer_data = pd.read_csv('historical-accredited-power-stations-and-projects-0.csv', encoding='latin-1')
    except UnicodeDecodeError:
        print("Latin-1 failed, trying cp1252 encoding...")
        cer_data = pd.read_csv('historical-accredited-power-stations-and-projects-0.csv', encoding='cp1252')
mapping_data = pd.read_csv('duid_to_power_station_mapping.csv', encoding='utf-8')

# Rename columns for clarity
aemo_data.columns = ['DUID', 'Energy_MWh', 'Avg_MW']
cer_data.columns = ['Power_Station_Name', 'Installed_Capacity_MW', 'Accreditation_Start_Date']
mapping_data.columns = ['DUID', 'Fuel_Source', 'State_Code', 'Station_Name']

# Clean state codes (remove the '1' at the end: VIC1 -> VIC)
mapping_data['State'] = mapping_data['State_Code'].str[:-1]

# Extract state from CER power station names (last element after split by ' - ')
def extract_state(name):
    parts = str(name).split(' - ')
    if len(parts) > 0:
        return parts[-1].strip()
    return None

cer_data['State'] = cer_data['Power_Station_Name'].apply(extract_state)

# Extract base name from CER (everything before the state)
def extract_base_name(name):
    parts = str(name).split(' - ')
    if len(parts) > 1:
        # Return everything except the last part (state)
        return ' - '.join(parts[:-1]).strip()
    return str(name).strip()

cer_data['Base_Name'] = cer_data['Power_Station_Name'].apply(extract_base_name)

print(f"Loaded {len(aemo_data)} AEMO records")
print(f"Loaded {len(cer_data)} CER records")
print(f"Loaded {len(mapping_data)} mapping records")

# Define renewable energy sources
RENEWABLE_SOURCES = [
    'Solar', 'Wind', 'Hydro', 'Battery', 'Biomass', 
    'Geothermal', 'Wave', 'Tidal', 'Renewable', 'Water', 'Bagasse', 'Biogas - sludge', 'Grid'
]

# Filter mapping data to only include renewables
print("\nFiltering for renewable energy sources only...")
def is_renewable(fuel_source):
    if pd.isna(fuel_source):
        return False
    fuel_source_str = str(fuel_source).lower()
    return any(renewable.lower() in fuel_source_str for renewable in RENEWABLE_SOURCES)

mapping_data['Is_Renewable'] = mapping_data['Fuel_Source'].apply(is_renewable)
renewable_duids = mapping_data[mapping_data['Is_Renewable']]['DUID'].tolist()

print(f"Found {len(renewable_duids)} renewable DUIDs out of {len(mapping_data)} total")

# Filter AEMO data to only include renewable DUIDs
aemo_data = aemo_data[aemo_data['DUID'].isin(renewable_duids)]
print(f"Filtered to {len(aemo_data)} renewable AEMO records")

# Function to find best match using fuzzy matching with state validation
def find_best_match(station_name, state, cer_df, threshold=70):
    # Filter CER data by state first
    state_matches = cer_df[cer_df['State'] == state]
    
    if len(state_matches) == 0:
        return None, 0
    
    # Use fuzzy matching on the base names
    choices = state_matches['Base_Name'].tolist()
    indices = state_matches.index.tolist()
    
    # Find best match
    result = process.extractOne(
        station_name, 
        choices, 
        scorer=fuzz.token_sort_ratio
    )
    
    if result and result[1] >= threshold:
        best_match_idx = indices[choices.index(result[0])]
        return best_match_idx, result[1]
    
    return None, 0

# Merge AEMO data with mapping data
print("\nMerging AEMO data with DUID mapping...")
merged = aemo_data.merge(mapping_data, on='DUID', how='left')

# Now match with CER data
print("Matching with CER data using fuzzy matching...")
results = []

for idx, row in merged.iterrows():
    result_row = {
        'DUID': row['DUID'],
        'Energy_MWh': row['Energy_MWh'],
        'Avg_MW': row['Avg_MW'],
        'Fuel_Source': row['Fuel_Source'],
        'State': row['State'],
        'AEMO_Station_Name': row['Station_Name'],
        'CER_Power_Station_Name': 'NA',
        'Installed_Capacity_MW': 'NA',
        'Accreditation_Start_Date': 'NA',
        'Match_Score': 0
    }
    
    # Try to find match in CER data
    if pd.notna(row['Station_Name']) and pd.notna(row['State']):
        match_idx, score = find_best_match(
            row['Station_Name'], 
            row['State'], 
            cer_data,
            threshold=70
        )
        
        if match_idx is not None:
            cer_row = cer_data.loc[match_idx]
            result_row['CER_Power_Station_Name'] = cer_row['Power_Station_Name']
            result_row['Installed_Capacity_MW'] = cer_row['Installed_Capacity_MW']
            result_row['Accreditation_Start_Date'] = cer_row['Accreditation_Start_Date']
            result_row['Match_Score'] = score
    
    results.append(result_row)
    
    # Progress indicator
    if (idx + 1) % 50 == 0:
        print(f"Processed {idx + 1}/{len(merged)} records...")

# Create final dataframe
final_df = pd.DataFrame(results)

# Print matching statistics
matched_count = len(final_df[final_df['CER_Power_Station_Name'] != 'NA'])
total_count = len(final_df)
print(f"\nMatching complete!")
print(f"Successfully matched: {matched_count}/{total_count} ({matched_count/total_count*100:.1f}%)")
print(f"Average match score: {final_df[final_df['Match_Score'] > 0]['Match_Score'].mean():.1f}")

# Save to CSV
output_csv = 'combined_renewable_energy_data.csv'
final_df.to_csv(output_csv, index=False)
print(f"\nCSV file saved: {output_csv}")

# Show sample of results
print("\nSample of matched records:")
print(final_df[final_df['CER_Power_Station_Name'] != 'NA'].head(10)[
    ['DUID', 'AEMO_Station_Name', 'CER_Power_Station_Name', 'Match_Score']
])

print("\nSample of unmatched records:")
unmatched = final_df[final_df['CER_Power_Station_Name'] == 'NA']
if len(unmatched) > 0:
    print(unmatched.head(5)[['DUID', 'AEMO_Station_Name', 'State']])
else:
    print("All records matched!")