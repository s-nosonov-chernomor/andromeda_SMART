# run.py
import uvicorn
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("alerts.telemetry").setLevel(logging.DEBUG)
logging.getLogger("automation").setLevel(logging.DEBUG)

uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)
