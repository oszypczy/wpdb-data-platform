"""Container healthcheck: Streamlit answers on /_stcore/health (exit 0) or not (exit 1).

The python:slim image has no curl. Health does not depend on the data: the app is
healthy with an empty gold layer and shows "czekam na dane" (07 §3).
"""

import sys
import urllib.request

try:
    with urllib.request.urlopen("http://localhost:8501/_stcore/health", timeout=4) as resp:
        sys.exit(0 if resp.status == 200 else 1)
except Exception as exc:  # noqa: BLE001 - any failure means unhealthy
    print(f"unhealthy: {exc}", file=sys.stderr)
    sys.exit(1)
