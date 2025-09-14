import pandas as pd
import glob
import os

# 📂 Folder path where your Excel files are stored
folder_path = r"C:\Users\medha\OneDrive\Desktop\Vaccination"   # <-- Change this path

# 🔍 Get all Excel files (.xlsx) in the folder
excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

# ✅ Check if files are found
if not excel_files:
    print("❌ No Excel files found in the folder!")
    exit()
else:
    print(f"✅ Found {len(excel_files)} Excel files:")
    for file in excel_files:
        print(" -", os.path.basename(file))

# 📄 Loop through each file one by one
for file in excel_files:
    try:
        # Read the Excel file
        df = pd.read_excel(file, engine="openpyxl")

        # Show file name, columns, and first 5 rows
        print(f"\n📂 Cleaning File: {os.path.basename(file)}")
        print("🔹 Columns:", list(df.columns))    # Show column names
        print("🔹 First 5 rows:\n", df.head())    # Show first 5 rows

        # Show shape before removing duplicates
        print("🔹 Shape before removing duplicates:", df.shape)

        # 🧹 Remove duplicate rows based on ALL columns
        cleaned_df = df.drop_duplicates()

        # Show shape after removing duplicates
        print("🔹 Shape after removing duplicates:", cleaned_df.shape)

        # 💾 Save the cleaned DataFrame to a **new Excel file**
        output_file = os.path.join(folder_path, f"cleaned_{os.path.basename(file)}")
        cleaned_df.to_excel(output_file, index=False)

        print(f"✅ Cleaned file saved as: {output_file}")

    except Exception as e:
        print(f"⚠️ Could not clean {os.path.basename(file)}: {e}")

print("\n🎉 All files cleaned successfully!")




