import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import log_startup_config
from backend.routers import health, market, mobile, strategy, system
from backend.services.scheduler_service import start_scheduler, stop_scheduler
from probability_engine.routers import probability_router


LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
file_handler = RotatingFileHandler(
    LOG_DIR / "fastapi.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(file_handler)

log_startup_config()

app = FastAPI(
    title="ETH Options Command Center API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(market.router)
app.include_router(mobile.router)
app.include_router(strategy.router)
app.include_router(system.router)
app.include_router(probability_router.router)


@app.on_event("startup")
def startup_event():
    start_scheduler()


@app.on_event("shutdown")
def shutdown_event():
    stop_scheduler()
