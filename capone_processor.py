import pandas as pd
import os
from datetime import datetime

# Vendor account mapping for CapOne
capone_account_map = {
    "NEW YORK EYE": 5000,
    "VISION-EASE LENS": 5000,
    "SEIKO OPTICAL PRODUCTS": 5000,
    "HEB": 7200,
    "H-E-B": 7200,
    "WALMART": 7200,
    "HOME DEPOT": 7300,
    "THE HOME DEPOT": 7300,
    "LOWES": 7300,
    "OPTICAL WORKS CORP": 7300,
    "PRISM TOOL COMPANY LLC": 7300,
    "POSTAL CENTER PLUS": 7210,
    "AMAZON": 7200,
    "AMAZON MARK": 7200,
    "INSURANCE": 6850,
    "TEMU.COM": 6050,
    "MY VISION EXPRESS": 7300,
    "ATT": 7600,
    "ATT BILL PAYMENT": 7600,
    "REMOTEPc": 7210,
}

UNCATEGORIZED_ACCOUNT = 7800

def map_account(desc):
    """
    Maps a transaction description to a MultiLedger account number.
    It checks if the description contains any of the predefined vendor strings.
    """
    if pd.isnull(desc):
        return UNCATEGORIZED_ACCOUNT
    desc_upper = str(desc).upper()
    # Sort by length descending to match longer strings first (e.g., 'THE HOME DEPOT' before 'HOME DEPOT')
    for vendor in sorted(capone_account_map.keys(), key=len, reverse=True):
        if vendor in desc_upper:
            return capone_account_map[vendor]
    return UNCATEGORIZED_ACCOUNT

def map_short_desc(desc):
    """
    Extracts the matched vendor name from the description for the Short Description field.
    """
    if pd.isnull(desc):
        return None
    desc_upper = str(desc).upper()
    # Sort by length descending for consistency
    for vendor in sorted(capone_account_map.keys(), key=len, reverse=True):
        if vendor in desc_upper:
            return vendor
    return desc

def process_capone_csv(capone_csv_path, output_folder, gj_startnum):
    """
    Reads Capital One CSV/Excel, processes transactions, maps vendors to accounts,
    and generates MultiLedger import files (and an unmapped report if necessary).
    """
    # Handles both CSV and common Excel extensions
    if capone_csv_path.lower().endswith(('.xlsx', '.xls')):
        df = pd.read_excel(capone_csv_path)
    else:
        # Default to CSV read
        df = pd.read_csv(capone_csv_path)

    # Standardize column names if needed (assuming 'Description', 'Debit', 'Credit', 'Transaction Date' are present)
    # Note: If the actual CapOne file has different headers, this part will need adjustment.

    # 1. Cleaning and Mapping
    df['Description_Clean'] = df['Description'].astype(str).str.replace(r'[^\w\s]', '', regex=True).str.strip()
    df['Account'] = df['Description_Clean'].apply(map_account)
    df['ShortDescription'] = df['Description_Clean'].apply(map_short_desc)
    
    # 2. Convert amounts to numeric, fill NaN with 0
    df['Debit'] = pd.to_numeric(df['Debit'], errors='coerce').fillna(0)
    df['Credit'] = pd.to_numeric(df['Credit'], errors='coerce').fillna(0)
    
    # Calculate net amount (assuming positive means expense/debit for this card process)
    # The actual amount is |Debit - Credit|. The Debit/Credit columns will be used later
    # to determine the sign for the double-entry format.
    df['Amount'] = (df['Debit'] - df['Credit']).abs()
    
    # Filter out zero-amount transactions (e.g., balance transfers, internal adjustments)
    df = df[df['Amount'] > 0].copy()

    # 3. Sort and Number
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
    df.sort_values('Transaction Date', inplace=True)
    
    # Assign GJ numbers incrementally based on sorted dates
    df = df.reset_index(drop=True)
    if len(df) > 0:
        df['Document'] = ['GJ#'+str(i) for i in range(gj_startnum, gj_startnum + len(df))]
    else:
        # Handle case where all transactions were filtered out
        timestamp = datetime.now().strftime('%Y%m%d')
        print("No transactions found after filtering.")
        return os.path.join(output_folder, f"CapOne_import_EMPTY_{timestamp}.csv"), None

    # 4. Prepare MultiLedger Import Data (Double-Entry Format)
    
    # CounterAccount fixed for CapOne VISA liability account (e.g., 2130)
    df['CounterAccount'] = 2130
    
    # Short description for export: use mapped short desc except for uncategorized,
    # where the original full description is used.
    df['ShortDescForExport'] = df.apply(
        lambda r: r['ShortDescription'] if r['Account'] != UNCATEGORIZED_ACCOUNT else r['Description'], 
        axis=1
    )
    
    # MultiLedger Import format requires two lines per transaction:
    # 1. Debit/Expense: Account (e.g., 5000), Amount (Positive)
    # 2. Credit/Liability: CounterAccount (2130), Amount (Negative)
    
    # Prepare the Expense/Debit line
    df_debit = df.copy()
    df_debit.rename(columns={'Account': 'AccountCode', 'Amount': 'AmountValue'}, inplace=True)
    df_debit['AccountCode_2'] = df_debit['CounterAccount']
    df_debit['AmountValue_2'] = -df_debit['AmountValue']

    # Final MultiLedger format columns:
    # 0: Date, 1: Document, 2: Account (Expense), 3: Description, 4: Amount (+),
    # 5: Account (Liability), 6: Description, 7: Amount (-)
    export_cols = [
        'Transaction Date', 'Document', 
        'AccountCode', 'ShortDescForExport', 'AmountValue', 
        'AccountCode_2', 'ShortDescForExport', 'AmountValue_2'
    ]
    df_output = df_debit[export_cols]
    
    # Rename columns for final CSV output
    df_output.columns = [
        'Date', 'Document', 
        'Account', 'Description_1', 'Amount_1', 
        'CounterAccount', 'Description_2', 'Amount_2'
    ]
    
    # 5. Export
    timestamp = datetime.now().strftime('%Y%m%d')
    base_csv_name = f"CapOne_import_{timestamp}"
    export_csv = os.path.join(output_folder, f"{base_csv_name}.csv")
    
    # Export main CSV (no header, no index)
    df_output.to_csv(export_csv, index=False, header=False)
    
    # 6. Create unknown vendor report
    uncategorized_df = df[df['Account'] == UNCATEGORIZED_ACCOUNT]
    if not uncategorized_df.empty:
        unmapped_csv = os.path.join(output_folder, f"CapOne_unmapped_{timestamp}.csv")
        # Include relevant data for mapping purposes
        unmapped_cols = ['Transaction Date', 'Description', 'Debit', 'Credit', 'Amount']
        uncategorized_df.to_csv(unmapped_csv, columns=unmapped_cols, index=False)
    else:
        unmapped_csv = None
    
    print(f"Processed {len(df)} CapOne transactions.")
    if unmapped_csv:
        print(f"Found {len(uncategorized_df)} uncategorized transactions. See {unmapped_csv}")
    else:
        print("All transactions categorized successfully.")
    
    return export_csv, unmapped_csv