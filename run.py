# run.py
import uvicorn
import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s.%(msecs)03d | +%(relativeCreated)dms | %(levelname)s | %(name)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    )
logging.getLogger("alerts.telemetry").setLevel(logging.INFO)
logging.getLogger("automation").setLevel(logging.INFO) # INFO DEBUG WARNING

uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=False)
