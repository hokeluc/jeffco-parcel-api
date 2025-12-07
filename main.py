from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from urllib import parse
from sqlalchemy import create_engine
from query import address_by_name, city_comps, property_distance_comps, neighborhood_comps, most_valuable_streets, property_type_counts_city, occupancy_counts_city, most_valuable_street_types
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
DB_PATH = "./parcels.db"
load_dotenv()

#IP address: 138.67.212.56

#updated to no longer use a deprecated function
@asynccontextmanager
async def lifespan(app):
    global engine
    login = parse.quote(str(os.getenv("DB_USERNAME")))
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
    
# http://localhost:8000/funfacts/streetvalue
@app.get("/funfacts/streetvalue")
def get_most_valuable_streets():
    try:
        df = most_valuable_streets(engine)
        df['street_value'] = df['street_value'].map(lambda x: str(x).strip())
        return df.drop('num_val', axis=1).to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# http://localhost:8000/funfacts/typevalue
@app.get("/funfacts/typevalue")
def get_most_valuable_street_types():
    try:
        df = most_valuable_street_types(engine)
        df['average_value'] = df['average_value'].map(lambda x: str(x).strip())
        return df.drop('num_val', axis=1).to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Endpoint to get property price + city stats
# Example:
# http://localhost:8000/city-comps?address=2104%20WASHINGTON%20AVE&city=GOLDEN
@app.get("/city-comps")
def get_city_comps(address: str, city: str):
    try:
        result = city_comps(engine, address, city)

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Property not found with that address and city."
            )

        return result

    except HTTPException:
        # re-raise clean 404s / etc.
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/neighborhood-comps")
def get_neighborhood_comps(address: str, neighborhood: str):
    try:
        result = neighborhood_comps(engine, address, neighborhood)

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Property not found with that address and neighborhood."
            )

        return result

    except HTTPException:
        # re-raise clean 404s / etc.
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# http://localhost:8000/property-distance-comps?address=2104%20WASHINGTON%20AVE&city=GOLDEN
@app.get("/property-distance-comps")
def get_property_distance_comps(
    address: str,
    city: str,
    radius_miles: float = 0.5,  # default radius
):
    try:
        result = property_distance_comps(engine, address, city, radius_miles)

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Property not found with that address and city."
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# http://localhost:8000/property-types-city?city=GOLDEN
@app.get("/property-types-city")
def get_property_types_city(city: str):
    try:
        result = property_type_counts_city(engine, city)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/occupancy-city")
def get_occupancy_city(city: str):
    try:
        result = occupancy_counts_city(engine, city)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
