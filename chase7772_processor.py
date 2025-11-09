import pandas as pd
import os
from datetime import datetime

# Vendor to account mapping specific for Chase 7772
chase_account_map = {
    "MARCOLIN USA": 5000,
    "TIKTOK SHOP": 7200,
    "DILLEY TRUCK STOP": 6800,
    "HILCOVISION.COM": 7200,
    "DAC VISION": 7200,
    "TIKTOK ADS": 6050,
    "TIKTOK PROMOTE": 6050,
    "H-E-B GAS": 6800,
    "H-E-B GASCARWASH": 6800,
    "MY VISION EXPRESS": 6650,
    "MY VISION EXPRESS LLC": 6650,
    "IN EYETOPIA OPTICS LLC": 5000,
    "IN EYETOPIA OPTICS": 5000,
    "ELEVENLABS": 6650,
    "DNHGODADDY.COM": 6050,
    "OPTISOURCE": 7200,
    "YOUNGER OPTICS": 5000,
    "7-ELEVEN": 6800,
    "KERING EYEWEAR": 5000,
    "RMA TOLL": 6250,
    "DEL NORTE OPTICAL": 5000,
    "CLARITI EYEWEAR INC": 5000,
    "CLARITI EYEWEAR": 5000,
    "AMAZON MKTPL": 7200,
    "AMAZON": 7200,
    "GOOGLE YOUTUBE": 6650,
    "GOOGLE PLAY YOUTUBE": 6650,
    "GOOGLE CANVA": 6650,
    "FACEBK": 6050,
    "FACEBOOK": 6050,
    "FACEBOOK ADVERTISING": 6050,
    "OPENAI": 6650,
    "DOMINOS": 6700,
    "MICROSOFT": 6650,
    "WORDPRESS": 6050,
    "SPECTRUM MOBILE": 7600,
    "SPECTRUM": 7700,
    "VALET TIPS": 6250,
    "8TO80 EYEWEAR": 5000,
    "EIGHT TO EIGHTY EYEWEAR": 5000,
    "KENMARK OPTICAL": 5000,
    "LUXOTTICA": 5000,
    "DEL MAR MINI STORAGE": 7250,
    "VALLEY SHREDDING": 6650,
    "CHICK-FIL-A": 6700,
    "EXXON": 6800,
    "SE GAS": 6800,
    "FUEL AMERICA": 6800,
    "CUSTOM BRAND": 5000,
    "HOKKAIDO": 6700,
    "YUMM": 6800,
    "SHELL OIL": 6800,

}

UNCATEGORIZED_ACCOUNT = 7800
CHASE_VISA_ACCOUNT = 2135
BANK_ACCOUNT = 1100

def map_account(desc):
    if pd.isnull(desc):
        return UNCATEGORIZED_ACCOUNT
    desc_upper = str(desc).upper()
    for vendor in sorted(chase_account_map.keys(), key=len, reverse=True):
        if vendor in desc_upper:
            return chase_account_map[vendor]
    if "AUTOMATIC PAYMENT" in desc_upper:
        return CHASE_VISA_ACCOUNT
    return UNCATEGORIZED_ACCOUNT

def map_short_desc(desc):
    if pd.isnull(desc):
        return None
    desc_upper = str(desc).upper()
    for vendor in sorted(chase_account_map.keys(), key=len, reverse=True):
        if vendor in desc_upper:
            return vendor
    if "AUTOMATIC PAYMENT" in desc_upper:
        return "AUTOMATIC PAYMENT"
    return desc

def process_chase7772_csv(chase_csv_path, output_folder, gj_startnum):
    df = pd.read_csv(chase_csv_path)
    
    # Convert date column to datetime (assumed 'Transaction Date' column, adjust if necessary)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
    
    # Clean description for mapping
    df['CleanDescription'] = df['Description'].str.replace(r'\W+', ' ', regex=True).str.strip()
    
    # Map accounts and short descriptions
    df['Account'] = df['CleanDescription'].apply(map_account)
    df['ShortDescription'] = df['CleanDescription'].apply(map_short_desc)
    
    # Convert Amount column (use 'Amount' or adjust column name as necessary)
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
    
    # Sort by date ascending for GJ numbering
    df.sort_values('Transaction Date', inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # Assign GJ numbers
    df['Document'] = ['GJ#' + str(gj_startnum + i) for i in range(len(df))]
    
    # Define counter accounts for two-sided posting
    df['CounterAccount'] = df.apply(lambda row: BANK_ACCOUNT if row['Amount'] < 0 else CHASE_VISA_ACCOUNT, axis=1)
    
    # Prepare negative amounts for double-entry (opposite side)
    df['AmountNeg'] = -df['Amount'].abs()
    
    # Prepare columns aligned with MultiLedger import format
    export_cols = ['Transaction Date', 'Document', 'Account', 'ShortDescription', 'Amount', 'CounterAccount', 'ShortDescription', 'AmountNeg']
    export_df = df[export_cols].copy()
    export_df.rename(columns={'Transaction Date': 'Date', 'ShortDescription': 'Description'}, inplace=True)
    
    # Save import CSV
    timestamp = datetime.now().strftime("%Y%m%d")
    output_csv_filename = f"Chase7772_import_{timestamp}.csv"
    output_csv_path = os.path.join(output_folder, output_csv_filename)
    export_df.to_csv(output_csv_path, index=False, header=False)
    
    # Save unmapped vendor CSV
    unmapped_df = df[df['Account'] == UNCATEGORIZED_ACCOUNT]
    unmapped_csv_path = None
    if not unmapped_df.empty:
        unmapped_csv_filename = f"Chase7772_unmapped_{timestamp}.csv"
        unmapped_csv_path = os.path.join(output_folder, unmapped_csv_filename)
        unmapped_df.to_csv(unmapped_csv_path, columns=['Transaction Date', 'Description', 'Amount'], index=False)
    
    return output_csv_path, unmapped_csv_path