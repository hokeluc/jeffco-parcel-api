from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

app = FastAPI()

#middle man for security, rn don't care about authentication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "courses.db"