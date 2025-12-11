from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from urllib import parse
from sqlalchemy import create_engine
from query import *
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
import re
import numpy as np
from pydantic import BaseModel
DB_PATH = "./parcels.db"
load_dotenv()


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
@app.get("/funfacts/streetvalue",
        summary = "Most Valuable Streets in Jefferson County",
        description = "Returns the 3 most valuable streets in Jefferson County by tax value.",
        )
def get_most_valuable_streets():
    try:
        df = most_valuable_streets(engine)
        df['street_value'] = df['street_value'].map(lambda x: str(x).strip())
        return df.drop('num_val', axis=1).to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# http://localhost:8000/funfacts/typevalue
@app.get("/funfacts/typevalue",
         summary = "Most Valuable Street Types in Jefferson County",
         description = "Returns the most valuable street types in Jefferson County by tax value."
         )
def get_most_valuable_street_types():
    try:
        df = most_valuable_street_types(engine)
        df['average_value'] = df['average_value'].map(lambda x: str(x).strip())
        return df.drop('num_val', axis=1).to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Endpoint to get property price + city stats
# Example:
# http://localhost:8000/city-comps?address=1100%2013TH%20ST&city=GOLDEN
@app.get("/city-comps",
         summary="Return Comparable Parcels for an Address/City",
         description="Return comparable parcels with valuation for a parcel's address and city in Jeffco.")

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
    
# http://localhost:8000/neighborhood-comps?address=1100%2013TH%20ST&neighborhood=Golden%20Proper
@app.get("/neighborhood-comps",
         summary="Return Comparable Parcels for a Neighborhood",
         description="Return comparable parcels with valuation for a given neighborhood in Jeffco.")
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
    
# http://localhost:8000/property-distance-comps?address=1100%2013TH%20ST&city=GOLDEN
@app.get("/property-distance-comps",
         summary="Return Comparable Parcels by Distance Jefferson County",
         description="Return comparable parcels by Euclidean distance with valuation for a parcel's address and city in Jeffco.")
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
@app.get("/property-types-city",
         summary="Return Property Types for a City",
         description="Return property types for a city within Jeffco boundaries.")
def get_property_types_city(city: str):
    try:
        result = property_type_counts_city(engine, city)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# http://localhost:8000/occupancy-city?city=GOLDEN
@app.get("/occupancy-city",
         summary="Return Occupancy Types for a City",
         description="Return occupancy types for a city within Jeffco boundaries.")
def get_occupancy_city(city: str):
    try:
        result = occupancy_counts_city(engine, city)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# http://localhost:8000/neighbors?pin=30-342-02-017
# http://localhost:8000/neighbors?address=512%2016TH%20STREET&city=GOLDEN
@app.get("/neighbors",
         summary="Return Neighbors for a Parcel by Address or PIN",
         description="Return neighbors for a parcel by parcel identification number or address and city in Jeffco.")
def get_neighbors(address: str or None = None, city: str or None = None, pin: str or None = None, limit: int = 50):
    if address and not city:  # a city must be provided for address filtering
        raise HTTPException(status_code=400,
                            detail="Please provide a city with the given address.")
    if city and not address:  # a city alone cannot be provided for address filtering
        raise HTTPException(status_code=400,
                            detail="Please provide an address with the given city.")
    if pin and (address or city):  # if address or city is provided along with pin
        raise HTTPException(status_code=400,
                            detail="Please provide either only a parcel pin or address + city for neighbor search.")
    if not pin and not (address and city):  # if neither of the three valid fields are provided
        raise HTTPException(status_code=400,
                            detail="Please provide either a parcel pin or address + city for neighbor search.")
    try:
        if (address and city) and not pin:
            df = neighbors_address(engine, address, city, limit)
            return df.replace({np.nan: 'N/A'}).to_dict(orient='records')
        elif pin and not (address and city):
            df = neighbors_parcel_pin(engine, pin, limit)
            return df.replace({np.nan: 'N/A'}).to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#http://localhost:8000/turnover/neighborhood?years=5
@app.get("/turnover/neighborhood",
         summary="Return Neighborhood Turnover over Time in Years",
         description="Return property turnover for a neighborhood in Jeffco over a specified amount of years (default 10.)")
def get_turnover_neighborhood(years: int = 10):
    try:
        df = turnover_neighborhood(engine, years)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#http://localhost:8000/turnover/subdivision?years=3
@app.get("/turnover/subdivision",
         summary="Return Subdivision Turnover over Time in Years",
         description="Return subdivision turnover for a neighborhood in Jeffco over a specified amount of years (default 10.)")
def get_turnover_subdivision(years: int = 10):
    try:
        df = turnover_subdivision(engine, years)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#http://localhost:8000/value-change/neighborhood
@app.get("/value-change/neighborhood",
         summary="Return Neighborhood Value Change",
         description="Return value changes for neighborhoods in Jeffco.")
def get_value_change_neighborhood():
    try:
        df = value_change_by_neighborhood(engine)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#who am I http://localhost:8000/whoami
@app.get("/whoami",
         summary="Return Authenticated User Name",
         description="Return the current authenticated user name.")
def whoami():
    try:
        return {"username": current_username(engine)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to add a starred parcel
@app.post("/parcels/add_starred",
         summary="Add a 'Starred' parcel to the database based on authenticated user.",
         description="Add favorite parcels by object ID to the database for easy access and lookup based on authenticated user.")
def create_star(object_id: str):
    try:
        n = add_parcel(engine, object_id)
        return {"ok": True, "rows_affected": n}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to modify a parcel's mailing address
@app.put("/parcels/edit_mailing",
         summary="Edit Parcel Mailing Address",
         description="Edit a parcel's mailing address for a given parcel identification number.")
def edit_mailing(parcel_pin: str, address: str, city: str, state: str, zip: str):
    DIRECTIONS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}

    STREET_TYPES = {
        "ST", "AVE", "BLVD", "RD", "LN", "DR", "CT", "PL", "PKWY", "WAY", "CIR",
        "HWY", "TER", "TRL", "RUN", "SQ", "CV", "CTX", "EXPY", "FWY", "PIKE",
    }

    SUFFIX_WORDS = {"APT", "UNIT", "SUITE", "STE", "FL", "BLDG"}

    tokens = address.strip().upper().split()

    street_num = None
    direction = None
    street_type = None
    suffix = None

    # regex for street num
    if tokens and re.match(r"^\d+[A-Z]?$", tokens[0]):  # handles 14, 14A, 2301B
        street_num = tokens.pop(0)
    else:
        raise HTTPException(status_code=400,
                        detail="Please provide a valid street number.")

    # validate direction if present
    if tokens and tokens[0] in DIRECTIONS:
        direction = tokens.pop(0)

    # validate suffix if present
    if tokens and tokens[-1] in SUFFIX_WORDS:
        suffix = tokens.pop()

    # validate street type
    if tokens and tokens[-1] in STREET_TYPES:
        street_type = tokens.pop()
    else:
        raise HTTPException(status_code=400,
                        detail="Please provide a valid street type.")

    if tokens:
        street_name = " ".join(tokens)
    else:
        raise HTTPException(status_code=400,
                        detail="Please provide a valid street name.")

    parts = zip.split("-", 1)
    zipcode5 = parts[0]
    zipcode4 = parts[1] if len(parts) == 2 else None


    try:
        return update_mailing_address(engine, parcel_pin, street_num, 
                               direction, street_name, street_type,
                               suffix, city, state, zipcode5, zipcode4)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/parcels/delete_starred",
         summary="Delete a 'Starred' parcel to the database based on authenticated user.",
         description="Delete favorite parcels by object ID to the database if current authenticated user starred parcel.")
def delete_starred(object_id: str):
    username = current_username(engine)
    result = delete_starred_parcels(username, object_id)
    if result == -1:
        raise HTTPException(status_code=400, detail="Parcel is not starred by current authenticated user.")
    else:
        return result

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

