import sqlite3
import pandas as pd
import os
from pathlib import Path

def create_households_table():
    """Create the households table if it doesn't exist."""
    database_path = 'foguthtradeticket.db'
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    # Create households table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS households (
            HouseholdId INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Households table created or already exists.")

def find_spreadsheet_file():
    """Find the spreadsheet file in the Downloads folder."""
    downloads_path = Path.home() / "Downloads"
    
    # Look for common spreadsheet file extensions
    spreadsheet_extensions = ['*.xlsx', '*.xls', '*.csv']
    
    for extension in spreadsheet_extensions:
        files = list(downloads_path.glob(extension))
        if files:
            # Return the most recently modified file
            latest_file = max(files, key=os.path.getmtime)
            print(f"Found spreadsheet: {latest_file}")
            return latest_file
    
    return None

def import_households_data(file_path=None):
    """Import household data from a spreadsheet file."""
    
    # Create the table first
    create_households_table()
    
    # If no file path provided, try to find one in Downloads
    if file_path is None:
        file_path = find_spreadsheet_file()
        if file_path is None:
            print("No spreadsheet file found in Downloads folder.")
            print("Please specify a file path or place a .xlsx, .xls, or .csv file in your Downloads folder.")
            return False
    
    try:
        # Read the spreadsheet based on file extension
        file_extension = Path(file_path).suffix.lower()
        
        if file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_extension == '.csv':
            df = pd.read_csv(file_path)
        else:
            print(f"Unsupported file type: {file_extension}")
            return False
        
        print(f"Successfully read {len(df)} rows from {file_path}")
        print(f"Columns found: {list(df.columns)}")
        
        # Check if required columns exist
        required_columns = ['HouseholdId', 'name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return False
        
        # Clean the data
        df_clean = df[required_columns].dropna()
        print(f"After cleaning: {len(df_clean)} rows to import")
        
        # Connect to database and import data
        database_path = 'foguthtradeticket.db'
        conn = sqlite3.connect(database_path)
        
        # Clear existing data (optional - remove this line if you want to append instead)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM households")
        print("Cleared existing households data")
        
        # Import new data
        df_clean.to_sql('households', conn, if_exists='append', index=False)
        
        # Verify the import
        cursor.execute("SELECT COUNT(*) FROM households")
        count = cursor.fetchone()[0]
        print(f"Successfully imported {count} households into the database")
        
        # Show a sample of imported data
        cursor.execute("SELECT * FROM households LIMIT 5")
        sample_data = cursor.fetchall()
        print("\nSample of imported data:")
        for row in sample_data:
            print(f"ID: {row[0]}, Name: {row[1]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error importing data: {str(e)}")
        return False

def list_households():
    """List all households in the database."""
    database_path = 'foguthtradeticket.db'
    
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM households ORDER BY HouseholdId")
        households = cursor.fetchall()
        
        if households:
            print(f"\nTotal households in database: {len(households)}")
            print("HouseholdId | Name")
            print("-" * 40)
            for household in households:
                print(f"{household[0]:10} | {household[1]}")
        else:
            print("No households found in database")
        
        conn.close()
        
    except Exception as e:
        print(f"Error listing households: {str(e)}")

if __name__ == "__main__":
    print("Foguth Trade Ticket - Household Data Importer")
    print("=" * 50)
    
    # You can specify a specific file path here, or let it auto-detect from Downloads
    # file_path = r"C:\Users\YourName\Downloads\your_spreadsheet.xlsx"
    file_path = None  # Auto-detect from Downloads folder
    
    success = import_households_data(file_path)
    
    if success:
        print("\n" + "=" * 50)
        list_households()
    else:
        print("\nImport failed. Please check the error messages above.")
        print("\nMake sure your spreadsheet has columns named 'HouseholdId' and 'name'")
        print("Supported file formats: .xlsx, .xls, .csv")
