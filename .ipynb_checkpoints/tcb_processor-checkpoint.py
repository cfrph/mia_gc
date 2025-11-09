import pandas as pd
import os
import tempfile
from datetime import datetime
# New Google Cloud Document AI Imports
from google.cloud import documentai
from google.api_core.client_options import ClientOptions
from PyPDF2 import PdfReader, PdfWriter # Still needed for local file operations and splitting (if necessary)
import re

# --- GOOGLE CLOUD CONFIGURATION (Sourced from Environment Variables) ---
# NOTE: Set these environment variables in your Google Cloud Run/App Engine deployment configuration.
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "linear-poet-477701-q6")
GCP_PROCESSOR_ID = os.environ.get("GCP_PROCESSOR_ID", "f5a3493251c3af8e")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us") # e.g., 'us', 'eu', 'asia'

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
    "ESSILOR SAFETY CCD": "ESSILOR SAFETY CCD",
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


def extract_tables_with_doc_ai(pdf_path):
    """
    Uses Google Cloud Document AI (General Processor) to extract tables from a PDF.
    
    NOTE: We rely on the Document Processor Service Client and the 'tables' feature.
    """
    if not GCP_PROJECT_ID or not GCP_PROCESSOR_ID:
        raise EnvironmentError(
            "GCP_PROJECT_ID or GCP_PROCESSOR_ID environment variables are not set. "
            "Cannot run Google Cloud Document AI."
        )

    processor_id = GCP_PROCESSOR_ID
    project_id = GCP_PROJECT_ID
    location = GCP_LOCATION
    
    # The Document AI client needs a regional endpoint if not 'us'
    client_options = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)
    
    # The full resource name of the processor
    name = client.processor_path(project_id, location, processor_id)

    # Read the file into memory
    with open(pdf_path, "rb") as image:
        image_content = image.read()

    # The Document AI General Processor works well for table extraction
    document = documentai.Document(
        content=image_content, 
        mime_type="application/pdf"
    )

    # Configure the request to use the tables feature
    request = documentai.ProcessRequest(
        name=name,
        document=document,
        # Enable table extraction
        process_options=documentai.ProcessOptions(
            table_extraction_params=documentai.TableExtractionParams(
                enabled=True
            )
        )
    )

    print(f"[DEBUG] Sending PDF to Document AI processor: {processor_id}...")
    
    # Process the document
    result = client.process_document(request=request)
    processed_document = result.document
    
    print(f"[DEBUG] Document AI processing complete. Found {len(processed_document.pages)} pages.")

    all_rows = []
    
    # Document AI table structure is slightly different. We need to iterate over all pages.
    for page in processed_document.pages:
        for table in page.tables:
            # Reconstruct the grid from table cells
            all_cells = table.body_rows + table.header_rows
            if not all_cells:
                continue

            max_row = max(cell.row_index for row in all_cells for cell in row.cells)
            max_col = max(cell.column_index for row in all_cells for cell in row.cells)
            grid = [["" for _ in range(max_col + 1)] for _ in range(max_row + 1)]
            
            for row in all_cells:
                for cell in row.cells:
                    # Get cell content using the layout text index
                    if cell.layout.text_anchor.text_segments:
                        start_index = cell.layout.text_anchor.text_segments[0].start_index
                        end_index = cell.layout.text_anchor.text_segments[0].end_index
                        content = processed_document.text[start_index:end_index].strip()
                        grid[cell.row_index][cell.column_index] = content
            
            all_rows.extend(grid)
            
    print(f"[DEBUG] Extracted total of {len(all_rows)} rows from all tables.")
    return all_rows

def fuzzy_header_match(row):
    """Checks if a row matches the expected header format (case and space insensitive)."""
    targets = [
        "DATE", "BUSINESS WEBSITE OR DESCRIPTION", "DEBITS", "CREDITS"
    ]
    # Ensure row has enough elements safely
    row_slice = row[:4] + [""] * max(0, 4 - len(row))
    
    norm = [str(cell).strip().upper().replace(" ", "") for cell in row_slice[:4]]
    targets_norm = [col.replace(" ", "") for col in targets]
    return norm == targets_norm

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    """
    Main function to process the TCB statement, extract data using Google Cloud Document AI, 
    map accounts, and generate CSV files for import.
    """
    # 1. Extract raw table data
    all_rows = extract_tables_with_doc_ai(pdf_path)

    # 2. Find Header and Build DataFrame
    header_idx = None
    for i, row in enumerate(all_rows):
        if fuzzy_header_match(row):
            header_idx = i
            break
    
    if header_idx is None:
        print("[ERROR] Table header row not found! Aborting.")
        raise ValueError("Can't locate table header in OCR output from Google Cloud Document AI.")
    
    # Filter data rows below the header
    table_data = []
    header_row = all_rows[header_idx]
    
    for row in all_rows[header_idx+1:]:
        if fuzzy_header_match(row):
            continue 
        if len(row) < len(header_row):
            row = row + [""] * (len(header_row) - len(row))
            
        norm0 = str(row[0]).strip().upper()
        if norm0 == "" or "TOTAL" in norm0 or len(row[0].strip()) == 0:
            continue
        table_data.append(row)
    
    # Normalize column names
    columns = [c.strip().upper() for c in header_row]
    columns = ["DATE" if c == "DATE"
               else "DESCRIPTION" if c == "BUSINESS WEBSITE OR DESCRIPTION"
               else c for c in columns]
    
    # Create DataFrame
    num_cols = len(columns)
    cleaned_data = [row[:num_cols] for row in table_data]
    df = pd.DataFrame(cleaned_data, columns=columns)

    # 3. Data Cleaning and Conversion
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    # Clean and convert amounts. Assuming Document AI outputs positive values for both columns.
    df["DEBITS"] = pd.to_numeric(
        df["DEBITS"].astype(str).str.replace(r'[$,() ]', '', regex=True).str.replace(r'^-', '', regex=True).apply(
            lambda x: float(x) if x else 0.0
        ), errors="coerce"
    )
    df["CREDITS"] = pd.to_numeric(
        df["CREDITS"].astype(str).str.replace(r'[$,() ]', '', regex=True).str.replace(r'^-', '', regex=True).apply(
            lambda x: float(x) if x else 0.0
        ), errors="coerce"
    )

    # 4. Segregate Debits and Credits
    # Debits: DEBITS column is greater than 0, and not a check.
    df_debits = df[
        (df["DEBITS"].notnull()) &
        (df["DEBITS"] > 0) &
        (~df["DESCRIPTION"].str.contains("DDA REGULAR CHECK", case=False, na=False))
    ].copy()
    # Credits: CREDITS column is greater than 0
    df_credits = df[df["CREDITS"].notnull() & (df["CREDITS"] > 0)].copy()

    # Sort for GJ/DP numbering by Date Ascending
    df_debits.sort_values("DATE", inplace=True)
    df_credits.sort_values("DATE", inplace=True)

    # 5. Map Accounts and Assign Documents
    
    # Debits (Expenses/Payments)
    df_debits["Account"] = df_debits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in debit_account_map.items() if k in str(desc)), "7800")
    )
    df_debits["ShortDescription"] = df_debits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((k for k in debit_account_map.keys() if k in str(desc)), desc)
    )
    df_debits["CounterAccount"] = "1100" # Bank Account
    df_debits["Document"] = [
        f"GJ#{i}" for i in range(gj_startnum, gj_startnum + len(df_debits))
    ]

    # Credits (Deposits/Income)
    df_credits["Account"] = "4000" # Default Revenue
    df_credits["CounterAccount"] = "1100" # Bank Account
    df_credits["ShortDescription"] = df_credits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in credit_account_map.items() if k in str(desc)), desc)
    )
    df_credits["Document"] = [
        f"DP#{i}" for i in range(dp_startnum, dp_startnum + len(df_credits))
    ]

    # 6. Validate Output Sums vs PDF Summary Line (Best effort check) - Retained original logic structure
    summary_row = None
    for row in reversed(all_rows):
        if "TOTAL" in str(row[0]).upper():
            summary_row = row
            break
    
    if summary_row is not None:
        total_debits = df_debits["DEBITS"].sum()
        total_credits = df_credits["CREDITS"].sum()
        s_debit, s_credit = None, None
        
        for val in summary_row:
            val_clean = str(val).replace("$", "").replace(",", "").strip()
            if re.match(r"^\d+(\.\d+)?$", val_clean):
                 try:
                     val_f = float(re.sub(r"[^\d.]", "", val_clean))
                     if s_debit is None or val_f < s_debit:
                         s_debit = val_f
                     if s_credit is None or val_f > s_credit:
                         s_credit = val_f
                 except:
                     pass
        
        is_debit_match = s_debit is not None and abs(s_debit - total_debits) < 1
        is_credit_match = s_credit is not None and abs(s_credit - total_credits) < 1

        if not is_debit_match or not is_credit_match:
            print(f"[WARNING] Output sums do NOT match PDF summary! ")
            print(f"  PDF Debits: {s_debit}, Output Debits: {total_debits} | ")
            print(f"  PDF Credits: {s_credit}, Output Credits: {total_credits}")
        else:
            print("[OK] Output transaction sums match PDF summary.")
    else:
        print("[NOTICE] No summary row detected in statement; cannot auto-validate sum.")

    # 7. EXPORT DEBITS (Debit Account to Credit Bank)
    debit_rows = []
    for _, row in df_debits.iterrows():
        # Use the DEBITS column value (now positive)
        amt = round(row["DEBITS"], 2)
        date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
        
        # Line 1: Debit Expense Account, Credit Bank Account
        debit_rows.append([
            date_str, row["Document"], row["Account"],
            row["ShortDescription"], amt,
            row["CounterAccount"], row["ShortDescription"], -amt
        ])
    
    df_export_debits = pd.DataFrame(debit_rows, columns=[
        "Date", "Document", "Account", "Description", "Amount",
        "CounterAccount", "Description2", "AmountNeg"
    ])
    debit_csv = os.path.join(output_folder, f"TCB_debits_{timestamp}.csv")
    df_export_debits.to_csv(debit_csv, index=False, header=False)

    # 8. EXPORT CREDITS (Debit Bank to Credit Revenue Account)
    credit_rows = []
    for _, row in df_credits.iterrows():
        # Use the CREDITS column value (now positive)
        amt = round(row["CREDITS"], 2)
        date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
        
        # Line 1: Debit Bank Account (CounterAccount), Credit Revenue Account
        credit_rows.append([
            date_str, row["Document"], row["CounterAccount"],
            row["ShortDescription"], amt,
            row["Account"], row["ShortDescription"], -amt
        ])
    
    df_export_credits = pd.DataFrame(credit_rows, columns=[
        "Date", "Document", "Account", "Description", "Amount",
        "CounterAccount", "Description2", "AmountNeg"
    ])
    credit_csv = os.path.join(output_folder, f"TCB_credits_{timestamp}.csv")
    df_export_credits.to_csv(credit_csv, index=False, header=False)

    # 9. EXPORT UNMAPPED
    unmapped_rows = []
    for _, row in df_debits.iterrows():
        if row["Account"] == "7800":
            date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
            amt = row["DEBITS"]
            unmapped_rows.append([date_str, row["DESCRIPTION"], amt, "7800"])
    
    df_unmapped = pd.DataFrame(unmapped_rows, columns=["Date", "Description", "Amount", "Account"])
    unmapped_csv = None
    if not df_unmapped.empty:
        unmapped_csv = os.path.join(output_folder, f"TCB_unmapped_{timestamp}.csv")
        df_unmapped.to_csv(unmapped_csv, index=False)
    
    return credit_csv, debit_csv, unmapped_csv if unmapped_csv else None