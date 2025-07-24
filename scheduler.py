# STEP 1: Import modules
import subprocess
import datetime
import sys
from send_alert import send_email_alert  # Optional email module

# STEP 2: Define pipeline steps
pipeline = [
    ("🔁 Running data_loader.py", ["python", "data_loader.py"]),
    ("🔧 Running transform.py", ["python", "transform.py"])
]

# STEP 3: Logging setup
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
log_path = "logs/pipeline_log.txt"

def log_message(msg):
    with open(log_path, "a") as log:
        log.write(f"{msg} @ {timestamp}\n")

# STEP 4: Execute pipeline steps
for label, cmd in pipeline:
    print(label)
    try:
        subprocess.check_call(cmd)
        log_message(f"✅ Success: {label}")
    except subprocess.CalledProcessError as e:
        log_message(f"❌ Failure: {label} - {e}")
        send_email_alert(f"{label} failed. See logs for details.")  # Optional
        sys.exit(1)

# STEP 5: Final success log
print("✅ HRIS pipeline completed successfully.")
log_message("✅ All steps completed successfully.")