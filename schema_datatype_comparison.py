

import pandas as pd

# Load the four CSV files
df1 = pd.read_csv('schema_info1.csv')
df2 = pd.read_csv('schema_info_03oct.csv')
df3 = pd.read_csv('schema_info_04oct.csv')
df4 = pd.read_csv('schema_info_05oct.csv')

# Strip whitespace from column names
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()
df3.columns = df3.columns.str.strip()
df4.columns = df4.columns.str.strip()

# Debug: Print column names and lengths of each DataFrame
print("Columns in df1:", df1.columns, "Length:", len(df1))
print("Columns in df2:", df2.columns, "Length:", len(df2))
print("Columns in df3:", df3.columns, "Length:", len(df3))
print("Columns in df4:", df4.columns, "Length:", len(df4))

# Perform a full outer merge on the 'Field' column across all DataFrames
merged_df = df1.merge(df2, on='Field', how='outer', suffixes=('_old', '_03oct'))
merged_df = merged_df.merge(df3, on='Field', how='outer', suffixes=('', '_04oct'))
merged_df = merged_df.merge(df4, on='Field', how='outer', suffixes=('', '_05oct'))

# Debug: Print columns after the merge
print("Columns after merging all DataFrames:", merged_df.columns)

# Fill missing 'Data Type' columns with 'No such field like that'
for column in ['Data Type_old', 'Data Type_03oct', 'Data Type_04oct', 'Data Type_05oct']:
    if column in merged_df.columns:
        merged_df[column] = merged_df[column].fillna('No such field like that')
    else:
        merged_df[column] = 'No such field like that'

# Debug: Print columns after filling missing values
print("Columns after filling missing values:", merged_df.columns)

# Count rows where 'Data Type' columns are not 'No such field like that'
row_count_no_nsf = {
    'Data Type_old': len(merged_df[merged_df['Data Type_old'] != 'No such field like that']),
    'Data Type_03oct': len(merged_df[merged_df['Data Type_03oct'] != 'No such field like that']),
    'Data Type_04oct': len(merged_df[merged_df['Data Type_04oct'] != 'No such field like that']),
    'Data Type_05oct': len(merged_df[merged_df['Data Type_05oct'] != 'No such field like that'])
}

# Print row counts before and after filtering 'No such field like that'
print("Row count before filtering 'No such field like that' for Data Type_old:", len(merged_df))
print("Row count before filtering 'No such field like that' for Data Type_03oct:", len(merged_df))
print("Row count before filtering 'No such field like that' for Data Type_04oct:", len(merged_df))
print("Row count before filtering 'No such field like that' for Data Type_05oct:", len(merged_df))

print("Row count without 'No such field like that' for Data Type_old:", row_count_no_nsf['Data Type_old'])
print("Row count without 'No such field like that' for Data Type_03oct:", row_count_no_nsf['Data Type_03oct'])
print("Row count without 'No such field like that' for Data Type_04oct:", row_count_no_nsf['Data Type_04oct'])
print("Row count without 'No such field like that' for Data Type_05oct:", row_count_no_nsf['Data Type_05oct'])

# Initialize a list to store fields with different data types
datatype_diffs = [['Field', 'Data Type_old', 'Data Type_03oct', 'Data Type_04oct', 'Data Type_05oct']]

# Iterate over the merged DataFrame rows to compare 'Data Type' columns
for _, row in merged_df.iterrows():
    # Check if there is a difference in any 'Data Type' across the columns
    if (row.get('Data Type_old') != row.get('Data Type_03oct')) or \
       (row.get('Data Type_old') != row.get('Data Type_04oct')) or \
       (row.get('Data Type_old') != row.get('Data Type_05oct')) or \
       (row.get('Data Type_03oct') != row.get('Data Type_04oct')) or \
       (row.get('Data Type_03oct') != row.get('Data Type_05oct')) or \
       (row.get('Data Type_04oct') != row.get('Data Type_05oct')):
        datatype_diffs.append([
            row['Field'],
            row.get('Data Type_old', 'No such field like that'),
            row.get('Data Type_03oct', 'No such field like that'),
            row.get('Data Type_04oct', 'No such field like that'),
            row.get('Data Type_05oct', 'No such field like that')
        ])

# Convert the list to a DataFrame
diffs_df = pd.DataFrame(datatype_diffs[1:], columns=datatype_diffs[0])  # Skipping the header row for DataFrame content

# Save the differences to a CSV file
diffs_df.to_csv('datatype_differences_across_four_files.csv', index=False)

# Print lengths of the original DataFrames and merged DataFrame
print("Length of df1:", len(df1))
print("Length of df2:", len(df2))
print("Length of df3:", len(df3))
print("Length of df4:", len(df4))
print("Length of merged_df:", len(merged_df))

print("Data type differences across four files have been saved to 'datatype_differences_across_four_files.csv'")
