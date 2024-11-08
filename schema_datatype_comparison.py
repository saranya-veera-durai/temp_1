
import pandas as pd
import os

# Directory containing CSV files
csv_directory = r'C:\New folder\DE\testing\ss'

# Initialize an empty list to hold DataFrames
merged_df = []

# Loop through all CSV files in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Add the 'filename' column before selecting specific columns
        df['filename'] = filename
        
        # Select only the columns that you want to merge vertically
        columns_to_merge = ['Field', 'Hierarchy', 'Data Type', 'filename']
        df = df[columns_to_merge]
        
        # Append the DataFrame to the list
        merged_df.append(df)

# Concatenate all DataFrames in the list
final_df = pd.concat(merged_df, ignore_index=True)

# Save the merged DataFrame to a new CSV file
final_df.to_csv(r'C:\New folder\DE\testing\mergeddddddddddddddd_file.csv', index=False)

print("CSV files merged successfully!")


###############################################################################




import pandas as pd
import os

# Directory containing CSV files
csv_directory = r'C:\New folder\DE\testing\ss'

# Initialize an empty list to hold DataFrames
dataframes = []

# Loop through all CSV files in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Add a new column 'filename' to keep track of the source file
        df['filename'] = filename
        
        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate all DataFrames in the list
merged_df = pd.concat(dataframes, ignore_index=True)

# Keep only relevant columns
merged_df = merged_df[['Field', 'Data Type', 'filename']]

# Group by Field and Data Type, aggregating filenames for each combination
grouped_df = merged_df.groupby(['Field', 'Data Type'])['filename'].apply(list).reset_index()

# Filter fields that have more than one unique Data Type
fields_with_diff_datatypes = grouped_df.groupby('Field').filter(lambda x: x['Data Type'].nunique() > 1)

# Print or save the summary
print("Fields with conflicting data types across files and corresponding filenames:")
print(fields_with_diff_datatypes)

# Optionally, save this summary to a new CSV
fields_with_diff_datatypes.to_csv(r'C:\New folder\DE\testing\fields_with_conflicting_data_types.csv', index=False)










