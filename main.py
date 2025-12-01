from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import sqlite3

app = FastAPI()

#middle man for security, rn don't care about authentication so allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


DB_PATH = "./parcels.db"


def get_connection(connDB=True):

    if not connDB:
        return None #add code to connect to database here, idk how to do that rn
    # connect to the sqlite database locally
    if connDB:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    return None

# Sample endpoint
@app.get("/")
def read_root():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM parcels LIMIT 10;")
    rows = cur.fetchall()
    return {"message": "Hello, World!", "data": [dict(row) for row in rows]}

