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
    """
    Uses Google Cloud Document AI to extract tables from a PDF.
    Assumes a processor with table extraction capability.
    """
    if not GCP_PROJECT_ID or not GCP_PROCESSOR_ID:
        raise EnvironmentError(
            "GCP_PROJECT_ID or GCP_PROCESSOR_ID environment variables are not set. Cannot run Google Cloud Document AI."
        )

    processor_id = GCP_PROCESSOR_ID
    project_id = GCP_PROJECT_ID
    location = GCP_LOCATION

    # Modern endpoint for Document AI v1 client
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
    for page in processed_document.pages:
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
            all_rows.extend(grid)
    print(f"[DEBUG] Extracted total of {len(all_rows)} rows from all tables.")
    return all_rows

def fuzzy_header_match(row):
    targets = ["DATE", "BUSINESS WEBSITE OR DESCRIPTION", "DEBITS", "CREDITS"]
    row_slice = row[:4] + [""] * max(0, 4 - len(row))
    norm = [str(cell).strip().upper().replace(" ", "") for cell in row_slice[:4]]
    targets_norm = [col.replace(" ", "") for col in targets]
    return norm == targets_norm

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    all_rows = extract_tables_with_doc_ai(pdf_path)

    header_idx = None
    for i, row in enumerate(all_rows):
        if fuzzy_header_match(row):
            header_idx = i
            break
    if header_idx is None:
        print("[ERROR] Table header row not found! Aborting.")
        raise ValueError("Can't locate table header in OCR output from Google Cloud Document AI.")
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
    columns = [c.strip().upper() for c in header_row]
    columns = ["DATE" if c == "DATE"
               else "DESCRIPTION" if c == "BUSINESS WEBSITE OR DESCRIPTION"
               else c for c in columns]
    num_cols = len(columns)
    cleaned_data = [row[:num_cols] for row in table_data]
    df = pd.DataFrame(cleaned_data, columns=columns)

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

    # Summary line validation
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
            print(f" PDF Debits: {s_debit}, Output Debits: {total_debits} | ")
            print(f" PDF Credits: {s_credit}, Output Credits: {total_credits}")
        else:
            print("[OK] Output transaction sums match PDF summary.")
    else:
        print("[NOTICE] No summary row detected in statement; cannot auto-validate sum.")

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
