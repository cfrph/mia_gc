from flask import Flask, render_template_string, request, send_from_directory
import os
from datetime import datetime


app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

HOME_HTML = """
<!DOCTYPE html>
<html><head><title>MultiLedger Import Tool</title></head><body style="font-family:Arial; margin:20px;">
<h2>MultiLedger Import Tool</h2>
<p>Select a statement type and upload your file.</p>

<form action="/upload_tcb" method="post" enctype="multipart/form-data" style="margin-bottom:40px;">
    <fieldset>
        <legend><strong>Texas Community Bank (PDF)</strong></legend>
        <label>Upload PDF: <input type="file" name="file" required></label><br><br>
        <label>Starting GJ Number: <input type="number" name="gj_startnum" required></label><br><br>
        <label>Starting DP Number: <input type="number" name="dp_startnum" required></label><br><br>
        <input type="submit" value="Process TCB Statement" style="background-color:#001f4d; color:white; padding:8px 16px; border:none; border-radius:4px;">
    </fieldset>
</form>

<form action="/upload_capone" method="post" enctype="multipart/form-data" style="margin-bottom:40px;">
    <fieldset>
        <legend><strong>Capital One Visa (CSV or Excel)</strong></legend>
        <label>Upload CSV/Excel: <input type="file" name="file" required></label><br><br>
        <label>Starting GJ Number: <input type="number" name="gj_startnum" required></label><br><br>
        <input type="submit" value="Process CapOne Statement" style="background-color:#001f4d; color:white; padding:8px 16px; border:none; border-radius:4px;">
    </fieldset>
</form>

<form action="/upload_chase7772" method="post" enctype="multipart/form-data" style="margin-bottom:40px;">
    <fieldset>
        <legend><strong>Chase Visa 7772 (CSV)</strong></legend>
        <label>Upload CSV: <input type="file" name="file" required></label><br><br>
        <label>Starting GJ Number: <input type="number" name="gj_startnum" required></label><br><br>
        <input type="submit" value="Process Chase 7772 Statement" style="background-color:#001f4d; color:white; padding:8px 16px; border:none; border-radius:4px;">
    </fieldset>
</form>

</body></html>
"""

SUCCESS_HTML = """
<!DOCTYPE html>
<html><head><title>Processing Complete</title></head><body style="font-family:Arial; margin:20px;">
<h2>{{ bank_name }} Statement Processed Successfully</h2>
<p>Download your MultiLedger import files below:</p>
<a href="/download/{{ import_file }}">Download Import CSV</a><br>
{% if unmapped_file %}
<a href="/download/{{ unmapped_file }}">Download Unmapped Vendors CSV</a>
{% endif %}
</body></html>
"""

@app.route("/")
def home():
    return render_template_string(HOME_HTML)

@app.route("/upload_tcb", methods=["POST"])
def upload_tcb():
    # Local import to avoid import-time failure if pandas or other deps are missing
    from tcb_processor import process_tcb_statement

    file = request.files["file"]
    gj_startnum = int(request.form["gj_startnum"])
    dp_startnum = int(request.form["dp_startnum"])
    filename = file.filename
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    timestamp = datetime.now().strftime("%Y%m%d")
    credit_csv, debit_csv, unmapped_csv = process_tcb_statement(
        pdf_path=filepath,
        gj_startnum=gj_startnum,
        dp_startnum=dp_startnum,
        output_folder=OUTPUT_FOLDER,
        timestamp=timestamp
    )
    return render_template_string(
        SUCCESS_HTML,
        bank_name="TCB",
        import_file=os.path.basename(credit_csv),
        unmapped_file=os.path.basename(unmapped_csv) if unmapped_csv else None
    )

@app.route("/upload_capone", methods=["POST"])
def upload_capone():
    from capone_processor import process_capone_csv

    file = request.files["file"]
    gj_startnum = int(request.form["gj_startnum"])
    filename = file.filename
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    timestamp = datetime.now().strftime("%Y%m%d")
    import_csv, unmapped_csv = process_capone_csv(
        capone_csv_path=filepath,
        output_folder=OUTPUT_FOLDER,
        gj_startnum=gj_startnum,
    )
    return render_template_string(
        SUCCESS_HTML,
        bank_name="CapOne",
        import_file=os.path.basename(import_csv),
        unmapped_file=os.path.basename(unmapped_csv) if unmapped_csv else None
    )

@app.route("/upload_chase7772", methods=["POST"])
def upload_chase7772():
    from chase7772_processor import process_chase7772_csv

    file = request.files["file"]
    gj_startnum = int(request.form["gj_startnum"])
    filename = file.filename
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    timestamp = datetime.now().strftime("%Y%m%d")
    import_csv, unmapped_csv = process_chase7772_csv(
        chase_csv_path=filepath,
        output_folder=OUTPUT_FOLDER,
        gj_startnum=gj_startnum,
    )
    return render_template_string(
        SUCCESS_HTML,
        bank_name="Chase 7772",
        import_file=os.path.basename(import_csv),
        unmapped_file=os.path.basename(unmapped_csv) if unmapped_csv else None
    )

@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(OUTPUT_FOLDER, filename, as_attachment=True)

@app.route("/_health_imports")
def health_imports():
    results = {}
    # Attempt to import key libraries and return versions or error messages
    try:
        import importlib
        for pkg in ("pandas", "numpy", "flask", "gunicorn"):
            try:
                mod = importlib.import_module(pkg)
                ver = getattr(mod, "__version__", "unknown")
                results[pkg] = ver
            except Exception as e:
                results[pkg] = f"error: {type(e).__name__} {str(e)}"
    except Exception as outer:
        results["health_check"] = f"failed: {type(outer).__name__} {str(outer)}"
    return results

if __name__ == "__main__":
    app.run(debug=True, port=5001)