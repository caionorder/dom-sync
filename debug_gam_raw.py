"""Script de diagnostico - bate no GAM e mostra o CSV cru para domain report"""
import gzip
import io
import ssl
import urllib.request
import time
from dotenv import load_dotenv
from googleads import ad_manager, oauth2
from datetime import datetime, timedelta, timezone
from config.settings import Config

load_dotenv()

NETWORK_CODE = "23154379558"  # Maturidade

# Auth
oauth2_client = oauth2.GoogleServiceAccountClient(
    key_file=Config.json_key_path, scope="https://www.googleapis.com/auth/dfp"
)
client = ad_manager.AdManagerClient(
    oauth2_client=oauth2_client,
    application_name="JoinAds-DOM",
    network_code=NETWORK_CODE,
)

# Datas - today
now = datetime.now(timezone(timedelta(hours=-3)))
today = now

report_query = {
    "dimensions": ["DATE", "AD_UNIT_NAME"],
    "columns": [
        "AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS",
        "AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS",
        "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
        "AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM",
    ],
    "reportCurrency": "USD",
    "dateRangeType": "CUSTOM_DATE",
    "adUnitView": "HIERARCHICAL",
    "startDate": {"year": today.year, "month": today.month, "day": today.day},
    "endDate": {"year": today.year, "month": today.month, "day": today.day},
}

print(f"Network: {NETWORK_CODE}")
print(f"Date range: {today.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
print(f"Query: {report_query}")
print()

# Run report
report_service = client.GetService("ReportService", version="v202602")
report_job = report_service.runReportJob({"reportQuery": report_query})
report_job_id = report_job["id"]

print(f"Report job ID: {report_job_id}")
print("Aguardando report...")

while True:
    status = report_service.getReportJobStatus(report_job_id)
    if status == "COMPLETED":
        break
    elif status == "FAILED":
        print("REPORT FAILED!")
        exit(1)
    time.sleep(0.5)

print("Report completo! Baixando CSV...")

# Download
download_url = report_service.getReportDownloadURL(report_job_id, "CSV_DUMP")

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

req = urllib.request.Request(download_url)
with urllib.request.urlopen(req, context=ssl_context) as response:
    compressed_data = response.read()

with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
    csv_raw = gz.read().decode("utf-8")

lines = csv_raw.split("\n")

print(f"\n{'='*80}")
print(f"TOTAL DE LINHAS (incl header): {len(lines)}")
print(f"{'='*80}")

# Header
print(f"\nHEADER: {lines[0]}")

# Primeiras 10 linhas de dados
print(f"\n--- Primeiras 10 linhas RAW ---")
for i, line in enumerate(lines[1:11], 1):
    print(f"  [{i}] {line}")

# Calcular revenue total RAW (sem dividir por micro)
print(f"\n{'='*80}")
print("ANALISE DE REVENUE")
print(f"{'='*80}")

headers = lines[0].split(",")
print(f"Headers: {headers}")

# Encontrar indice da coluna de revenue
rev_idx = None
for i, h in enumerate(headers):
    if "REVENUE" in h:
        rev_idx = i
        print(f"Revenue column index: {i} ({h})")
        break

if rev_idx is not None:
    total_raw = 0
    total_micro = 0.0
    row_count = 0
    for line in lines[1:]:
        if not line.strip():
            continue
        cols = line.split(",")
        if len(cols) > rev_idx:
            try:
                raw_val = int(cols[rev_idx])
                total_raw += raw_val
                total_micro += raw_val / 1_000_000
                row_count += 1
            except ValueError:
                pass

    print(f"\nTotal de linhas com dados: {row_count}")
    print(f"Revenue SOMA RAW (sem conversao):     {total_raw:,}")
    print(f"Revenue SOMA / 1,000,000 (micro):     ${total_micro:,.6f}")
    print(f"Revenue SOMA / 1,000 (milli):         ${total_raw / 1_000:,.2f}")
    print(f"Revenue SOMA como centavos (/100):    ${total_raw / 100:,.2f}")
    print(f"Revenue SOMA sem conversao (raw=USD): ${total_raw:,.2f}")
