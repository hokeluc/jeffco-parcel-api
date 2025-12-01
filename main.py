from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from urllib import parse
from sqlalchemy import create_engine
from query import address_by_name
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
DB_PATH = "./parcels.db"
load_dotenv()

#updated to no longer use a deprecated function
@asynccontextmanager
async def lifespan(app):
    global engine
    login = input("Login username: ")
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    engine = create_engine(f'postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403')
    yield

app = FastAPI(lifespan=lifespan)

#middle man for security, rn don't care about authentication so allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint to get owners by name: example : http://localhost:8000/owners?name=Smith
@app.get("/owners")
def get_owners(name: str):
    try:
        df = address_by_name(engine, name)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
