import pandas as pd
import os
from datetime import datetime
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions

# --- Cloud and Processor Config ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "linear-poet-477701-q6")
GCP_PROCESSOR_ID = os.environ.get("GCP_PROCESSOR_ID", "6df7b78a3654d182")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us")

# --- Account Mappings ---
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

def extract_bank_statement_entities(pdf_path):
    client_options = ClientOptions(api_endpoint=f"{GCP_LOCATION}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)
    name = client.processor_path(GCP_PROJECT_ID, GCP_LOCATION, GCP_PROCESSOR_ID)

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
    )
    result = client.process_document(request=request)
    document = result.document

    transactions = []
    for entity in document.entities:
        if entity.type_ == "transaction":
            txn = {"date": "", "description": "", "amount": "", "type": ""}
            for prop in entity.properties:
                if hasattr(prop, 'type_') and hasattr(prop, 'mention_text'):
                    if prop.type_ == "date":
                        txn["date"] = prop.mention_text
                    elif prop.type_ == "description":
                        txn["description"] = prop.mention_text
                    elif prop.type_ == "amount":
                        txn["amount"] = prop.mention_text
                    elif prop.type_ == "transaction_type":
                        txn["type"] = prop.mention_text
            transactions.append(txn)
    print("[DEBUG] First 3 transactions:", transactions[:3])
    return transactions

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    txns = extract_bank_statement_entities(pdf_path)
    df = pd.DataFrame(txns)

    # Safeguard: Ensure expected columns exist
    for col in ("date", "amount", "description", "type"):
        if col not in df.columns:
            df[col] = ""

    df["DATE"] = pd.to_datetime(df["date"], errors="coerce")
    df["AMOUNT"] = pd.to_numeric(df["amount"].astype(str).str.replace(r'[^\d\-.]', '', regex=True), errors="coerce")
    df["DESCRIPTION"] = df["description"]
    df["TYPE"] = df["type"].str.lower()

    # Process debits
    df_debits = df[df["TYPE"] == "debit"].copy()
    df_debits.sort_values("DATE", inplace=True)
    df_debits["Account"] = df_debits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in debit_account_map.items() if k in desc), "7800")
    )
    df_debits["ShortDescription"] = df_debits["DESCRIPTION"]
    df_debits["CounterAccount"] = "1100"
    df_debits["Document"] = [f"GJ#{i}" for i in range(gj_startnum, gj_startnum + len(df_debits))]

    debit_rows = [
        [
            row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else "",
            row["Document"], row["Account"], row["ShortDescription"], abs(row["AMOUNT"]),
            row["CounterAccount"], row["ShortDescription"], -abs(row["AMOUNT"])
        ] for _, row in df_debits.iterrows()
    ]
    df_export_debits = pd.DataFrame(debit_rows, columns=[
        "Date", "Document", "Account", "Description", "Amount",
        "CounterAccount", "Description2", "AmountNeg"
    ])
    debit_csv = os.path.join(output_folder, f"TCB_debits_{timestamp}.csv")
    df_export_debits.to_csv(debit_csv, index=False, header=False)

    # Process credits
    df_credits = df[df["TYPE"] == "credit"].copy()
    df_credits.sort_values("DATE", inplace=True)
    df_credits["Account"] = "4000"
    df_credits["CounterAccount"] = "1100"
    df_credits["ShortDescription"] = df_credits["DESCRIPTION"].str.upper().apply(
        lambda desc: next((v for k, v in credit_account_map.items() if k in desc), desc)
    )
    df_credits["Document"] = [f"DP#{i}" for i in range(dp_startnum, dp_startnum + len(df_credits))]

    credit_rows = [
        [
            row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else "",
            row["Document"], row["CounterAccount"], row["ShortDescription"], row["AMOUNT"],
            row["Account"], row["ShortDescription"], -abs(row["AMOUNT"])
        ] for _, row in df_credits.iterrows()
    ]
    df_export_credits = pd.DataFrame(credit_rows, columns=[
        "Date", "Document", "Account", "Description", "Amount",
        "CounterAccount", "Description2", "AmountNeg"
    ])
    credit_csv = os.path.join(output_folder, f"TCB_credits_{timestamp}.csv")
    df_export_credits.to_csv(credit_csv, index=False, header=False)

    # Export unmapped debits
    unmapped_rows = []
    for _, row in df_debits.iterrows():
        if row["Account"] == "7800":
            date_str = row["DATE"].strftime("%m/%d/%y") if pd.notnull(row["DATE"]) else ""
            unmapped_rows.append([date_str, row["DESCRIPTION"], abs(row["AMOUNT"]), "7800"])
    df_unmapped = pd.DataFrame(unmapped_rows, columns=["Date", "Description", "Amount", "Account"])

    unmapped_csv = None
    if not df_unmapped.empty:
        unmapped_csv = os.path.join(output_folder, f"TCB_unmapped_{timestamp}.csv")
        df_unmapped.to_csv(unmapped_csv, index=False)

    return credit_csv, debit_csv, unmapped_csv if unmapped_csv else None
