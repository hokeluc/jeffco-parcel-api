from urllib import parse
from sqlalchemy import create_engine, Engine
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# these can be globals defined in another file
schema = 'kkubaska'
table = 'jeffco_staging'

def address_by_name(engine: Engine, name: str):
    # should be updated, I think concating a null field makes the whole result null, so won't work for single owner homes
    query = f"""
    select
        ownnam || '|' || ownnam2 as owners,
        prpaddress || ', ' || prpctynam || ', ' || prpzip5 as address
    from {schema}.{table}
    where ownnam ilike '%%' || %s || '%%' or ownnam2 ilike '%%' || %s || '%%';
    """
    return pd.read_sql(
        query,
        engine,
        params=(name, name))


# Endpoint for city wide comps
def city_comps(engine: Engine, address: str, city: str):
    global schema, table

    if schema:
        full_table = f'"{schema}"."{table}"'
    else:
        full_table = f'"{table}"'

    # Define column names
    address_col = "prpaddress"
    city_col = "prpctynam"
    price_col = "valact"

    # 1) Find the specific property by address and its city
    prop_query = f"""
        SELECT
        {address_col} AS address,
        {city_col}    AS city,
        ({price_col}::numeric) AS price
    FROM {full_table}
    WHERE UPPER(TRIM({address_col})) = UPPER(TRIM(%s))
      AND UPPER(TRIM({city_col}))    = UPPER(TRIM(%s))
    LIMIT 1;
"""

    prop_df = pd.read_sql_query(prop_query, engine, params=(address, city))

    # If property not found, return None
    if prop_df.empty:
        return None

    prop_row = prop_df.iloc[0]

    # 2) Compute city wide stats for comp analysis
    stats_query = f"""
        SELECT
        MIN({price_col}::numeric) AS min_price,
        MAX({price_col}::numeric) AS max_price,
        MAX({price_col}::numeric) - MIN({price_col}::numeric) AS price_range,
        AVG({price_col}::numeric) AS avg_price,
        COUNT({price_col}) AS num_properties
    FROM {full_table}
    WHERE UPPER(TRIM({city_col})) = UPPER(TRIM(%s))
      AND {price_col} IS NOT NULL;
"""

    stats_df = pd.read_sql_query(stats_query, engine, params=(city,))

    # If no stats found (e.g., no properties with price), return property info with null stats
    if stats_df.empty:
        return {
            "property": {
                "address": prop_row["address"],
                "city": prop_row["city"],
                "price": float(prop_row["price"]) if pd.notna(prop_row["price"]) else None,
            },
            "city_stats": None,
        }

    stats_row = stats_df.iloc[0]

    result = {
        "property": {
            "address": prop_row["address"],
            "city": prop_row["city"],
            "price": float(prop_row["price"]) if pd.notna(prop_row["price"]) else None,
        },
        "city_stats": {
            "min_price": float(stats_row["min_price"]) if pd.notna(stats_row["min_price"]) else None,
            "max_price": float(stats_row["max_price"]) if pd.notna(stats_row["max_price"]) else None,
            "price_range": float(stats_row["price_range"]) if pd.notna(stats_row["price_range"]) else None,
            "avg_price": float(stats_row["avg_price"]) if pd.notna(stats_row["avg_price"]) else None,
            "num_properties": int(stats_row["num_properties"]) if pd.notna(stats_row["num_properties"]) else 0,
        },
    }

    return result

# Endpoint for radius based comps
def property_distance_comps(
    engine: Engine,
    address: str,
    city: str,
    radius_miles: float = 0.5,
):
    global schema, table

    if schema:
        full_table = f'"{schema}"."{table}"'
    else:
        full_table = f'"{table}"'

    address_col = "prpaddress"
    city_col    = "prpctynam"
    price_col   = "valact"
    x_col       = "x_coord"
    y_col       = "y_coord"

    # x/y are in feet → convert miles to feet
    radius_feet = radius_miles * 5280.0

    prop_query = f"""
        SELECT
            {address_col} AS address,
            {city_col}    AS city,
            ({price_col}::numeric)         AS price,
            ({x_col}::double precision)    AS x,
            ({y_col}::double precision)    AS y
        FROM {full_table}
        WHERE UPPER(TRIM({address_col})) = UPPER(TRIM(%s))
          AND UPPER(TRIM({city_col}))    = UPPER(TRIM(%s))
          AND {x_col} IS NOT NULL
          AND {y_col} IS NOT NULL
        LIMIT 1;
    """

    prop_df = pd.read_sql_query(prop_query, engine, params=(address, city))

    if prop_df.empty:
        return None

    prop_row = prop_df.iloc[0]
    x0 = float(prop_row["x"])
    y0 = float(prop_row["y"])

    comps_query = f"""
        SELECT
            {address_col} AS address,
            {city_col}    AS city,
            ({price_col}::numeric)         AS price,
            ({x_col}::double precision)    AS x,
            ({y_col}::double precision)    AS y,
            sqrt(
                power(({x_col}::double precision) - %s, 2) +
                power(({y_col}::double precision) - %s, 2)
            ) AS distance_feet
        FROM {full_table}
        WHERE {price_col} IS NOT NULL
          AND {x_col} IS NOT NULL
          AND {y_col} IS NOT NULL
          AND sqrt(
                power(({x_col}::double precision) - %s, 2) +
                power(({y_col}::double precision) - %s, 2)
              ) <= %s
          AND NOT (
                UPPER(TRIM({address_col})) = UPPER(TRIM(%s))
            AND UPPER(TRIM({city_col}))    = UPPER(TRIM(%s))
          )
        ORDER BY distance_feet ASC
        LIMIT 50;
    """

    comps_df = pd.read_sql_query(
        comps_query,
        engine,
        params=(x0, y0, x0, y0, radius_feet, address, city)
    )

    if comps_df.empty:
        comp_stats = {
            "min_price": None,
            "max_price": None,
            "avg_price": None,
            "num_properties": 0,
        }
        comparables = []
    else:
        comps_df["distance_miles"] = comps_df["distance_feet"] / 5280.0

        prices = comps_df["price"].dropna()

        comp_stats = {
            "min_price": float(prices.min()),
            "max_price": float(prices.max()),
            "price_range": float(prices.max() - prices.min()),
            "avg_price": float(prices.mean()),
            "num_properties": int(len(prices)),
        }

        comparables = [
            {
                "address": row["address"],
                "city": row["city"],
                "price": float(row["price"]),
                "distance_miles": float(row["distance_miles"]),
            }
            for _, row in comps_df.iterrows()
        ]

    return {
        "property": {
            "address": prop_row["address"],
            "city": prop_row["city"],
            "price": float(prop_row["price"]) if pd.notna(prop_row["price"]) else None,
        },
        "comp_stats": comp_stats,
        "comparables": comparables,
    }

# Endpoint for neighborhood comps
# Endpoint for city wide comps
def neighborhood_comps(engine: Engine, address: str, neighborhood: str):
    global schema, table

    if schema:
        full_table = f'"{schema}"."{table}"'
    else:
        full_table = f'"{table}"'

    # Define column names
    address_col = "prpaddress"
    neighborhood_col = "nhdnam"
    price_col = "valact"  

    # 1) Find the specific property by address and its city
    prop_query = f"""
        SELECT
        {address_col} AS address,
        {neighborhood_col}    AS neighborhood,
        ({price_col}::numeric) AS price
    FROM {full_table}
    WHERE UPPER(TRIM({address_col})) = UPPER(TRIM(%s))
      AND UPPER(TRIM({neighborhood_col}))    = UPPER(TRIM(%s))
    LIMIT 1;
"""

    prop_df = pd.read_sql_query(prop_query, engine, params=(address, neighborhood))

    # If property not found, return None
    if prop_df.empty:
        return None

    prop_row = prop_df.iloc[0]

    # 2) Compute city wide stats for comp analysis
    stats_query = f"""
        SELECT
        MIN({price_col}::numeric) AS min_price,
        MAX({price_col}::numeric) AS max_price,
        MAX({price_col}::numeric) - MIN({price_col}::numeric) AS price_range,
        AVG({price_col}::numeric) AS avg_price,
        COUNT({price_col}) AS num_properties
    FROM {full_table}
    WHERE UPPER(TRIM({neighborhood_col})) = UPPER(TRIM(%s))
      AND {price_col} IS NOT NULL;
"""

    stats_df = pd.read_sql_query(stats_query, engine, params=(neighborhood,))

    # If no stats found (e.g., no properties with price), return property info with null stats
    if stats_df.empty:
        return {
            "property": {
                "address": prop_row["address"],
                "neighborhood": prop_row["neighborhood"],
                "price": float(prop_row["price"]) if pd.notna(prop_row["price"]) else None,
            },
            "neighborhood_stats": None,
        }

    stats_row = stats_df.iloc[0]

    result = {
        "property": {
            "address": prop_row["address"],
            "neighborhood": prop_row["neighborhood"],
            "price": float(prop_row["price"]) if pd.notna(prop_row["price"]) else None,
        },
        "neighborhood_stats": {
            "min_price": float(stats_row["min_price"]) if pd.notna(stats_row["min_price"]) else None,
            "max_price": float(stats_row["max_price"]) if pd.notna(stats_row["max_price"]) else None,
            "price_range": float(stats_row["price_range"]) if pd.notna(stats_row["price_range"]) else None,
            "avg_price": float(stats_row["avg_price"]) if pd.notna(stats_row["avg_price"]) else None,
            "num_properties": int(stats_row["num_properties"]) if pd.notna(stats_row["num_properties"]) else 0,
        },
    }

    return result

def property_type_counts_city(engine: Engine, city: str):

    global schema, table

    if schema:
        full_table = f'"{schema}"."{table}"'
    else:
        full_table = f'"{table}"'

    # Jeffco columns – tweak if needed
    city_col = "prpctynam"      # city / place name
    property_type_col = "ownico"  # land use / property type description

    query = f"""
        SELECT
            {property_type_col} AS property_type,
            COUNT(*) AS count
        FROM {full_table}
        WHERE UPPER(TRIM({city_col})) = UPPER(TRIM(%s))
        GROUP BY {property_type_col}
        ORDER BY count DESC;
    """

    df = pd.read_sql_query(query, engine, params=(city,))

    # Convert to simple list of dicts
    type_counts = [
        {
            "property_type": row["property_type"],
            "count": int(row["count"]),
        }
        for _, row in df.iterrows()
    ]

    return {
        "city": city,
        "property_type_counts": type_counts,
    }

def occupancy_counts_city(engine: Engine, city: str):
    """
    Count parcels in a city by occupancy_type using concatenated + normalized
    property and mailing addresses.

    Rules:
      - commercial: ownico IS NOT NULL
      - owner_occupied: mailing addr matches property addr (normalized) OR mailing is empty
      - rental: everything else
    """

    global schema, table

    if schema:
        full_table = f'"{schema}"."{table}"'
    else:
        full_table = f'"{table}"'

    # Column names – tweak if your Jeffco schema differs
    city_col        = "prpctynam"

    prp_addr_col  = "prpstrnum"
    prp_street_col  = "prpstrnam"
    prp_city_col = "prpctynam"

    mail_addr1_col  = "mailstrnbr"
    mail_street_col   = "mailstrnam"
    mail_city_col  = "mailctynam"

    ownico_col      = "ownico"

    query = f"""
        WITH normalized AS (
            SELECT
                *,
                -- Concatenate + normalize property address
                UPPER(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE({prp_addr_col}, '') || ' ' ||
                            COALESCE({prp_street_col}, '') || ' ' ||
                            COALESCE({prp_city_col}, '')
                        ),
                        '\\s+',
                        ' '
                    )
                ) AS prop_addr_norm,

                -- Concatenate + normalize mailing address
                UPPER(
                    REGEXP_REPLACE(
                        TRIM(
                            COALESCE({mail_addr1_col}, '') || ' ' ||
                            COALESCE({mail_street_col}, '')  || ' ' ||
                            COALESCE({mail_city_col}, '')
                        ),
                        '\\s+',
                        ' '
                    )
                ) AS mail_addr_norm
            FROM {full_table}
        )
        SELECT
            CASE
                WHEN {ownico_col} IS NOT NULL THEN 'commercial'
                WHEN mail_addr_norm = '' THEN 'owner_occupied'
                WHEN mail_addr_norm = prop_addr_norm THEN 'owner_occupied'
                ELSE 'rental'
            END AS occupancy_type,
            COUNT(*) AS count
        FROM normalized
        WHERE UPPER(TRIM({city_col})) = UPPER(TRIM(%s))
        GROUP BY occupancy_type
        ORDER BY occupancy_type;
    """

    df = pd.read_sql_query(query, engine, params=(city,))

    results = [
        {"occupancy_type": row["occupancy_type"], "count": int(row["count"])}
        for _, row in df.iterrows()
    ]

    return {
        "city": city,
        "occupancy_counts": results,
    }

def most_valuable_streets(engine: Engine):
    """Returns (3) rows of: street_value (a comma seperated string), street_name, and num_val (the numerical street value)"""
    query = f"""
    select 
    distinct to_char(sum(totactval::numeric) over (partition by prpstrnam), '999,999,999,999') as street_value, 
    prpstrnam as street_name, 
    sum(totactval::numeric) over (partition by prpstrnam) as num_val
    from {schema}.{table}
    order by num_val desc
    limit 3;
    """
    return pd.read_sql(query, engine)

def most_valuable_street_types(engine: Engine):
    """Returns rows in order of value of: average_value (a comma seperated string), street_type, and num_val (the numerical average street type value)"""
    query = f"""
    select distinct to_char(avg(totactval::numeric) over (partition by prpstrtyp), '999,999,999,999') as average_value,
    prpstrtyp as street_type,
    avg(totactval::numeric) over (partition by prpstrtyp) as num_val
    from {schema}.{table}
    order by num_val desc;
    """
    return pd.read_sql(query, engine)

def main():
    login = input("Login username: ")
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    engine = create_engine(f'postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403')

    results = address_by_name(engine, 'mcdonald')
    print(results)
    
if __name__ == "__main__":
    main()
