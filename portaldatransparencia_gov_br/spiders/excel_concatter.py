import pandas as pd
import glob

if __name__ == '__main__':
    # Step 1: Get a list of all Excel file paths
    excel_files = glob.glob("path_to_files/*.xlsx")  # Adjust the path and extension if needed

    # Step 2: Read all Excel files and store them in a list of DataFrames
    dfs = [pd.read_excel(file) for file in excel_files]

    # Step 3: Concatenate all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

    # Step 4: Save the combined DataFrame to a new Excel file
    combined_df.to_excel("combined_output.xlsx", index=False)
