
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
