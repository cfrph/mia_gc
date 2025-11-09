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
GCP_LOCATION = os.environ.get("GCP_LOCATION", "us") # e.g. 'us', 'eu', 'asia'
# -----------------------------------

# ... (rest of configuration and utility functions remain the same) ...

def extract_tables_with_doc_ai(pdf_path):
    # ... (code for client setup and reading PDF content remains the same) ...
    client_options = ClientOptions(api_endpoint=f"{GCP_LOCATION}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=client_options)

    name = client.processor_path(GCP_PROJECT_ID, GCP_LOCATION, GCP_PROCESSOR_ID)
    
    with open(pdf_path, "rb") as image:
        image_content = image.read()
    
    raw_document = documentai.RawDocument(
        content=image_content,
        mime_type="application/pdf",
    )

    # --- UPDATED: Use set_included_fields to request table output ---
    process_options = documentai.ProcessOptions(
        # Retain native parsing for better OCR quality on digital PDFs
        ocr_config=documentai.OcrConfig(
            enable_native_pdf_parsing=True # Improves digital PDF accuracy
        ),
        # Explicitly request the tables output in the response structure
        set_included_fields=["pages.tables", "pages.page_number", "text"],
    )
    # ----------------------------------------------------------------

    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document,
        process_options=process_options # <-- Now uses the safer ProcessOptions
    )
    print(f"[DEBUG] Sending PDF to Document AI processor: {GCP_PROCESSOR_ID} requesting table output...")
    result = client.process_document(request=request)

    # ... (rest of the function remains the same) ...
    dfs = []
    for page in result.document.pages:
        for table in page.tables:
            header = [get_text_from_dimensions(result.document, h) for h in table.header_rows[0].cells]
            body = []
            for row in table.body_rows:
                body.append([get_text_from_dimensions(result.document, c) for c in row.cells])
            
            if body:
                df = pd.DataFrame(body, columns=header)
                dfs.append(df)
    
    return dfs

def process_tcb_statement(pdf_path, gj_startnum, dp_startnum, output_folder, timestamp):
    
    dfs = extract_tables_with_doc_ai(pdf_path)
    
    if not dfs:
        raise ValueError("Document AI did not return any structured tables from the PDF.")
# ... (rest of the file remains the same) ...