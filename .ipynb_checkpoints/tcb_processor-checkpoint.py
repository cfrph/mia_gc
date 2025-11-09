import pandas as pd
import os
from datetime import datetime
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import re
import tempfile 

# --- GOOGLE CLOUD CONFIGURATION ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "linear-poet-477701-q6")
GCP_PROCESSOR_ID = os.environ.get("GCP_PROCESSOR_ID", "f5a3493251c3af8e")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us")
# -----------------------------------

# Mapping for Credit transactions (Deposits/Income)
credit_account_map = {
    "MERCHANT BANKCD DEPOSIT ACH ENTRY MEMO POSTED TODAY": "DEP MERCH BANK CCD MAIN",
    "SYNCHRONY BANK MTOT DEP ACH ENTRY MEMO POSTED TODAY": "SYNCHRONY BANK CCD",
    "DEPOSIT MERCHANT BANKCD CCD": "DEP MERCH BANK CCD MAIN",
    "REGULAR DEPOSIT": "REGULAR DEPOSIT",
    "CIGNA": "FAA - CIGNA CCD",
    "EYEMED VISION CCD": "EYEMED VISION CCD",
    "ESSILOR": "ESSILOR SAFETY CCD",
    "EYETOPIA": "REIMB EYETOPIA CCD",
    "FAA ADMIN CCD": "FAA ADMIN CCD",
    "MTOT DEP SYNCHRONY BANK CCD": "SYNCHRONY BANK CCD",
    "HCCLAIMPMT SUPERIOR VISION CCD": "SUPERIOR VISION CCD",
    "HCCLAIMPMT UHC SPECTERA VSN CCD": "UHC SPECTERA VISION",
    "DAVIS VISION CCD": "DAVIS VISION CCD",
    "1010877933 FAA - AETNA CCD": "FAA - AETNA CCD",
    "FSL ADMIN FAA CCD": "FSL ADMIN FAA CCD",
}

# Mapping for Debit transactions (Expenses/Payments)
debit_account_map = {
    "USATAXPYMT IRS CCD": "2060",
    "USA TAX PYMT IRS CCD": "2060",
    "USA TAX PYMT IRS": "2060",
    "AUTOPAYBUS CHASE CREDIT CRD PPD": "2130",
    "CRCARDPMT CAPITAL ONE CCD": "2130",
    "CAPITAL ONE VISA PMT": "2130",
    "CHASE VISA PMT 7772": "2135",
    "CHASE VISA PMT 3506": "2136",
    "DISCOUNT MERCHANT BANKCD CCD": "6460",
    "DISCT MERCH BANK CCD": "6460",
    "MERCH BANK DISCT ACH": "6460",
    "FEE MERCHANT BANKCD CCD": "6460",
    "FEE MERCH BANK CCD": "6460",
    "MERCH BANK FEE ACH": "6460",
    "INTERCHNG MERCHANT BANKCD CCD": "6460",
    "INTERCHNG MERCH BANK CCD": "6460",
    "INTERCHNG MERCH BANK": "6460",
    "MERCH BANK INTERCHNG ACH": "6460",
    "WEB PAY PECAA BUYING GRP": "5000",
    "PECAA BUYING GRP": "5000",
    "ADT ALARM": "7475",
    "EVERON ALARM": "7475",
    "IPAY BILL PAY": "6200",
    "MBFS.COM MERCEDES LEASE": "7260",
    "TX WORKFORCE COMM": "2070",
}

# --- Utility Functions for Document AI ---

def get_text_from_dimensions(document, layout):
    """Extracts text from the Document object based on layout bounding box."""
    # This is a safe helper function to get text content from a layout/cell object
    text_segments = []
    for segment in layout.text_anchor.text_segments:
        start_index = int(segment.start_index)
        end_index = int(segment.end_index)
        text_segments.append(document.text[start_index:end_index])
    return "".join(text_segments).strip()

def extract_tables_with_doc_ai(pdf_path):
    """
    Sends the PDF to Google Cloud Document AI to extract table data.
    Returns a list of DataFrames, one for each table found.
    """
    if not GCP_PROJECT_ID or not GCP_PROCESSOR_ID:
        raise EnvironmentError(
            "GCP_PROJECT_ID or GCP_PROCESSOR_ID environment variables are not set. Cannot run Google Cloud Document AI."
        )
    processor_id = GCP_PROCESSOR_ID
    project_id = GCP_PROJECT_ID
    location = GCP_LOCATION

    client_options = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)
    name = client.processor_path(project_id, location, processor_id)

    with open(pdf_path, "rb") as image:
        image_content = image.read()
    raw_document = documentai.RawDocument(
        content=image_content,
        mime_type="application/pdf",
    )
    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document
    )
    print(f"[DEBUG] Sending PDF to Document AI processor: {processor_id}...")
    result = client.process_document(request=request)
    processed_document = result.document
    print(f"[DEBUG] Document AI processing complete. Found {len(processed_document.pages)} pages.")

    dataframes = []
    
    if processed_document.pages:
        for page_num, page in enumerate(processed_document.pages):
            for table_num, table in enumerate(page.tables):
                
                # Extract header row from the first row of header_rows (if present)
                if not table.header_rows:
                    print(f"[DEBUG] Page {page_num+1}, Table {table_num+1}: No explicit header rows found, attempting to use first body row as header.")
                    if not table.body_rows:
                        continue
                    # If no explicit header, assume the first body row is the header
                    header_text = [
                        get_text_from_dimensions(processed_document, cell.layout)
                        for cell in table.body_rows[0].cells
                    ]
                    body_rows_data = table.body_rows[1:] # Use subsequent rows as data
                else:
                    header_text = [
                        get_text_from_dimensions(processed_document, cell.layout)
                        for cell in table.header_rows[0].cells
                    ]
                    body_rows_data = table.body_rows
                
                # Extract body rows data
                clean_body_rows = []
                for row in body_rows_data:
                    row_data = [
                        get_text_from_dimensions(processed_document, cell.layout)
                        for cell in row.cells
                    ]
                    clean_body_rows.append(row_data)
                    
                # Create DataFrame immediately
                if clean_body_rows and header_text:
                    # Use raw header text for column names temporarily
                    clean_headers = [h.strip() for h in header_text]
                    # Ensure headers are unique, if Document AI duplicated them
                    unique_headers = []
                    for h in clean_headers:
                        count = 1
                        original_h = h
                        while h in unique_headers:
                            h = f"{original_h}_{count}"
                            count += 1
                        unique_headers.append(h)

                    df = pd.DataFrame(clean_body_rows, columns=unique_headers)
                    dataframes.append(df)
                    print(f"[DEBUG] Page {page_num+1}, Table {table_num+1} extracted. Headers: {unique_headers}")
                
    return dataframes

def clean_string(s):
    """Clean string by lowercasing and removing non-alphanumeric characters."""
    if not isinstance(s, str):
        return ""
    # Remove all non-alphanumeric characters and lowercase
    return re.sub(r'[^a-z0-9]', '', s.lower())

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    
    dfs = extract_tables_with_doc_ai(pdf_path)
    
    if not dfs:
        raise ValueError("Document AI did not return any structured tables from the PDF.")

    df = pd.concat(dfs, ignore_index=True)
    
    # 1. Implement Highly Flexible Fuzzy Column Identification (Tailored for 3-Column TCB)
    
    # Redefined tokens for the specific 3-column layout (Date, Description, Amount/Credit/Debit Combined)
    FUZZY_COLUMN_TOKENS = {
        'DATE': ['date', 'txn date', 'posted'],
        'DESCRIPTION': ['description', 'website', 'details', 'activity'],
        # Target the combined 'DEBITS/CREDITS' column or any variation of 'Amount'
        'AMOUNT_COMBINED': ['debit', 'credit', 'amount', 'balance', 'debtscredits'], 
    }
    REQUIRED_COLUMNS = list(FUZZY_COLUMN_TOKENS.keys())

    # Prepare extracted column names for matching (clean_name: original_name)
    cleaned_cols = {clean_string(col): col for col in df.columns.tolist()}
    col_rename = {}
    found_cols = set()
    
    # Iterate through required standard columns and find the best match
    for standard_name, search_terms in FUZZY_COLUMN_TOKENS.items():
        found = False
        for search_term in search_terms:
            cleaned_search_term = clean_string(search_term)
            
            # Find the first cleaned column name that *contains* the cleaned search term
            matching_cleaned_col = next((
                cleaned_name
                for cleaned_name in cleaned_cols.keys()
                if cleaned_search_term in cleaned_name and cleaned_search_term != ''
            ), None)
            
            if matching_cleaned_col:
                original_col_name = cleaned_cols[matching_cleaned_col]
                col_rename[original_col_name] = standard_name
                found_cols.add(standard_name)
                # Remove the found column from the search pool to prevent re-matching
                del cleaned_cols[matching_cleaned_col] 
                found = True
                break
    
    # CRITICAL CHECK: Ensure all required columns were found
    if not all(col in found_cols for col in REQUIRED_COLUMNS):
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in found_cols]
        print(f"[ERROR] Missing required columns: {missing_cols}. Available headers (cleaned): {list(df.columns)}")
        raise ValueError(
            "Can't locate table header in OCR output from Google Cloud Document AI. "
            f"Missing required columns: {', '.join(missing_cols)}. The PDF may not be the expected 3-column TCB format."
        )

    # Apply renames and keep only the 3 standardized columns
    df.rename(columns=col_rename, inplace=True)
    df = df[REQUIRED_COLUMNS].copy() 
    
    # Rename the combined amount column for clarity in subsequent steps
    df.rename(columns={'AMOUNT_COMBINED': 'Amount'}, inplace=True)


    # 2. Data Cleaning and Amount Consolidation
    
    # Clean up currency symbols and convert to numeric
    def clean_amount(series):
        # Remove commas, currency symbols, and handle negative signs/parentheses
        cleaned = series.astype(str).str.replace(r'[$, ]', '', regex=True).str.strip()
        # Handle the common case where a debit is represented as negative
        cleaned = cleaned.str.replace(r'\(', '-', regex=True).str.replace(r'\)', '', regex=True)
        return pd.to_numeric(cleaned, errors='coerce').fillna(0)
    
    # Since we have only one column, apply the cleaning directly to it
    df['Amount'] = clean_amount(df['Amount'])

    # Clean up Date column
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
    
    # Final data filtering
    df.dropna(subset=['DATE', 'DESCRIPTION'], inplace=True) 
    df = df[df['Amount'] != 0].sort_values('DATE').reset_index(drop=True)
    
    if df.empty:
        # Check if the remaining transactions are all zero or were dropped
        raise ValueError("No valid transactions found after cleaning and filtering. Check source PDF for data quality.")

    # 3. Apply Categorization and Document Numbers
    
    # Re-apply account mapping logic using the user's defined maps
    UNCATEGORIZED_ACCOUNT = "7800"
    CHECKING_ACCOUNT = "1100"
    INCOME_ACCOUNT = "4000"
    
    def map_debit_account(desc):
        desc_upper = str(desc).upper()
        return next((v for k, v in debit_account_map.items() if k in desc_upper), UNCATEGORIZED_ACCOUNT)

    # Assign Expense (Debit) account for outflows (Amount < 0), or Income (4000) for inflows (Amount > 0)
    df['Account'] = df.apply(lambda row: map_debit_account(row['DESCRIPTION']) if row['Amount'] < 0 else INCOME_ACCOUNT, axis=1) 
    
    # ShortDescription mapping: find the longest matching key from either map
    # Note: We don't distinguish between credit/debit map keys here, relying on longest match
    all_map_keys = {**debit_account_map, **{v: k for k, v in credit_account_map.items()}} # Combined maps
    
    def get_short_description(desc):
        desc_upper = str(desc).upper()
        # Find the longest matching key for better precision
        best_match = ""
        # Check debit map keys (these often have longer, more specific names)
        for k in debit_account_map.keys():
            if k in desc_upper and len(k) > len(best_match):
                best_match = k
        # Check credit map keys (can override if a longer match is found)
        for k in credit_account_map.keys():
            if k in desc_upper and len(k) > len(best_match):
                best_match = k
        return best_match if best_match else desc
    
    df['ShortDescription'] = df['DESCRIPTION'].apply(get_short_description)

    # 4. Split and Export Debits (Outflows/GJ) and Credits (Inflows/DP)

    df_credits = df[df['Amount'] > 0].copy() # Deposits/Inflows
    df_debits = df[df['Amount'] < 0].copy() # Withdrawals/Outflows

    # --- Process Credits (Deposits/Inflows - DP) ---
    credit_csv = None
    if not df_credits.empty:
        df_credits['AmountAbs'] = df_credits['Amount'].abs()
        df_credits['Document'] = [f"DP#{dp_startnum + i}" for i in range(len(df_credits))]
        
        # MultiLedger Import Format for Credit/Deposit (DP)
        credit_export_df = pd.DataFrame({
            'Date': df_credits['DATE'].dt.strftime('%m/%d/%Y'),
            'Document': df_credits['Document'],
            'Account': CHECKING_ACCOUNT, # Debit the Checking Account (1100)
            'Description': df_credits['ShortDescription'],
            'Amount': df_credits['AmountAbs'],
            'CounterAccount': INCOME_ACCOUNT, # Credit the Income Account (4000)
            'ShortDescription_Copy': df_credits['ShortDescription'],
            'AmountNeg': -df_credits['AmountAbs']
        })
        credit_csv = os.path.join(output_folder, f"TCB_deposits_{timestamp}.csv")
        credit_export_df.to_csv(credit_csv, index=False, header=False)

    # --- Process Debits (Withdrawals/Outflows - GJ) ---
    debit_csv = None
    if not df_debits.empty:
        df_debits['AmountAbs'] = df_debits['Amount'].abs()
        df_debits['Document'] = [f"GJ#{gj_startnum + i}" for i in range(len(df_debits))]
        
        # MultiLedger Import Format for Debit (GJ)
        debit_export_df = pd.DataFrame({
            'Date': df_debits['DATE'].dt.strftime('%m/%d/%Y'),
            'Document': df_debits['Document'],
            'Account': df_debits['Account'], # Debit the Expense/Vendor Account
            'Description': df_debits['ShortDescription'],
            'Amount': df_debits['AmountAbs'],
            'CounterAccount': CHECKING_ACCOUNT, # Credit the Checking Account (1100)
            'ShortDescription_Copy': df_debits['ShortDescription'],
            'AmountNeg': -df_debits['AmountAbs']
        })
        debit_csv = os.path.join(output_folder, f"TCB_withdrawals_{timestamp}.csv")
        debit_export_df.to_csv(debit_csv, index=False, header=False)

    # 5. Generate Unmapped Report
    unmapped_df = df[(df['Account'] == UNCATEGORIZED_ACCOUNT) & (df['Amount'] < 0)].copy() # Only unmapped DEBITS
    unmapped_csv = None
    if not unmapped_df.empty:
        unmapped_csv_filename = f"TCB_unmapped_{timestamp}.csv"
        unmapped_csv = os.path.join(output_folder, unmapped_csv_filename)
        unmapped_df['TransactionType'] = 'Debit'
        unmapped_df['AbsoluteAmount'] = unmapped_df['Amount'].abs()
        unmapped_df.to_csv(unmapped_csv, columns=['DATE', 'DESCRIPTION', 'AbsoluteAmount', 'TransactionType', 'Account'], index=False)

    return credit_csv, debit_csv, unmapped_csv if unmapped_csv else None