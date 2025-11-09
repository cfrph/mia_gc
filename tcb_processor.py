import pandas as pd
import os
import tempfile
from datetime import datetime
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
from PyPDF2 import PdfReader, PdfWriter
import re

# --- GOOGLE CLOUD CONFIGURATION ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "linear-poet-477701-q6")
GCP_PROCESSOR_ID = os.environ.get("GCP_PROCESSOR_ID", "f5a3493251c3af8e")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us") # e.g. 'us', 'eu', 'asia'

# Account mappings (partial for brevity)
credit_account_map = {
    # ... your mappings here ...
}
debit_account_map = {
    # ... your mappings here ...
}

def extract_tables_with_doc_ai(pdf_path):
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

    all_rows = []
    for page_num, page in enumerate(processed_document.pages):
        print(f"[DEBUG] Processing page {page_num + 1}")
        for table in page.tables:
            all_cells = table.header_rows + table.body_rows
            if not all_cells:
                continue
            max_row = max(cell.row_index for row in all_cells for cell in row.cells)
            max_col = max(cell.column_index for row in all_cells for cell in row.cells)
            grid = [["" for _ in range(max_col + 1)] for _ in range(max_row + 1)]
            for row in all_cells:
                for cell in row.cells:
                    if cell.layout.text_anchor.text_segments:
                        start_index = cell.layout.text_anchor.text_segments[0].start_index
                        end_index = cell.layout.text_anchor.text_segments[0].end_index
                        content = processed_document.text[start_index:end_index].strip()
                        grid[cell.row_index][cell.column_index] = content
            print(f"[DEBUG] Table on page {page_num + 1}:")
            for rnum, row in enumerate(grid):
                print(f"[DEBUG][Page {page_num + 1}][Row {rnum}]: {row}")
            all_rows.extend(grid)
    print(f"[DEBUG] Extracted total of {len(all_rows)} rows from all tables.")
    return all_rows

def fuzzy_header_match(row):
    # Acceptable header keywords
    header_keywords = ["DATE", "BUSINESS", "DESCRIPTION", "DEBITS", "CREDITS"]
    cells = [str(cell).upper().replace(" ", "") for cell in row]
    matches = sum(any(keyword in cell for cell in cells) for keyword in header_keywords)
    return matches >= 3  # Accept if at least three keywords match

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    all_rows = extract_tables_with_doc_ai(pdf_path)

    # Print all extracted rows for debug
    for i, row in enumerate(all_rows):
        print(f"[DEBUG] AllRows[{i}]: {row}")

    # Find all header rows (indexes)
    header_indexes = [i for i, row in enumerate(all_rows) if fuzzy_header_match(row)]
    if not header_indexes:
        print("[ERROR] Table header row not found! Aborting.")
        raise ValueError("Can't locate table header in OCR output from Google Cloud Document AI.")

    print(f"[DEBUG] Header row(s) found at indexes: {header_indexes}")

    # Use the columns from the first header as main columns
    header_row = all_rows[header_indexes[0]]
    columns = [c.strip().upper() for c in header_row]
    columns = ["DATE" if c == "DATE"
               else "DESCRIPTION" if ("BUSINESS" in c or "DESCRIPTION" in c)
               else c for c in columns]
    num_cols = len(columns)

    # Collect data rows skipping any repeated header rows
    table_data = []
    for i, row in enumerate(all_rows):
        if fuzzy_header_match(row):
            print(f"[DEBUG] Skipping header row at index {i}: {row}")
            continue
        # Skip rows above the first header
        if i < header_indexes[0]:
            continue
        if len(row) < num_cols:
            row = row + [""] * (num_cols - len(row))
        norm0 = str(row[0]).strip().upper()
        if norm0 == "" or "TOTAL" in norm0 or len(row[0].strip()) == 0:
            continue
        table_data.append(row[:num_cols])

    print(f"[DEBUG] Final table data count: {len(table_data)}")

    df = pd.DataFrame(table_data, columns=columns)

    # Data cleaning & conversion
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
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

    # Debits: DEBITS > 0 && not check
    df_debits = df[
        (df["DEBITS"].notnull()) &
        (df["DEBITS"] > 0) &
        (~df["DESCRIPTION"].str.contains("DDA REGULAR CHECK", case=False, na=False))
    ].copy()
    # Credits: CREDITS > 0
    df_credits = df[df["CREDITS"].notnull() & (df["CREDITS"] > 0)].copy()

    df_debits.sort_values("DATE", inplace=True)
    df_credits.sort_values("DATE", inplace=True)

    # Map accounts
    df_debits["Account"] = df_debits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in debit_account_map.items() if k in str(desc)), "7800")
    )
    df_debits["ShortDescription"] = df_debits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((k for k in debit_account_map.keys() if k in str(desc)), desc)
    )
    df_debits["CounterAccount"] = "1100"
    df_debits["Document"] = [
        f"GJ#{i}" for i in range(gj_startnum, gj_startnum + len(df_debits))
    ]

    df_credits["Account"] = "4000"
    df_credits["CounterAccount"] = "1100"
    df_credits["ShortDescription"] = df_credits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in credit_account_map.items() if k in str(desc)), desc)
    )
    df_credits["Document"] = [
        f"DP#{i}" for i in range(dp_startnum, dp_startnum + len(df_credits))
    ]

    # Export debits (Debit Account to Credit Bank)
    debit_rows = []
    for _, row in df_debits.iterrows():
        amt = round(row["DEBITS"], 2)
        date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
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

    # Export credits (Debit Bank to Credit Revenue Account)
    credit_rows = []
    for _, row in df_credits.iterrows():
        amt = round(row["CREDITS"], 2)
        date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
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

    # Export unmapped
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
