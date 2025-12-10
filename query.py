from urllib import parse
from sqlalchemy import create_engine, Engine, text, types
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# these can be globals defined in another file
schema = 'kkubaska'
parcels = 'jeffco_staging'
stars = 'starred_parcel'

def address_by_name(engine: Engine, name: str):
    # should be updated, I think concating a null field makes the whole result null, so won't work for single owner homes
    query = f"""
    select
        ownnam || '|' || ownnam2 as owners,
        prpaddress || ', ' || prpctynam || ', ' || prpzip5 as address
    from {schema}.{parcels}
    where ownnam ilike '%%' || %s || '%%' or ownnam2 ilike '%%' || %s || '%%';
    """
    return pd.read_sql(
        query,
        engine,
        params=(name, name))


# Endpoint for city wide comps
def city_comps(engine: Engine, address: str, city: str):
    global schema, parcels

    if schema:
        full_table = f'"{schema}"."{parcels}"'
    else:
        full_table = f'"{parcels}"'

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
    global schema, parcels

    if schema:
        full_table = f'"{schema}"."{parcels}"'
    else:
        full_table = f'"{parcels}"'

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
def neighborhood_comps(engine: Engine, address: str, neighborhood: str):
    global schema, parcels

    if schema:
        full_table = f'"{schema}"."{parcels}"'
    else:
        full_table = f'"{parcels}"'

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
    """ Not as useful as hoped, show the count of properties each company has within a city """

    global schema, parcels

    if schema:
        full_table = f'"{schema}"."{parcels}"'
    else:
        full_table = f'"{parcels}"'

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
    How categorize occupancy types:
      commercial: ownico IS NOT NULL
      owner_occupied: mailing addr matches property addr (normalized) OR mailing is empty
      rental: everything else
    """

    global schema, parcels

    if schema:
        full_table = f'"{schema}"."{parcels}"'
    else:
        full_table = f'"{parcels}"'

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
    from {schema}.{parcels}
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
    from {schema}.{parcels}
    order by num_val desc;
    """
    return pd.read_sql(query, engine)

def neighbors_parcel_pin(engine: Engine, parcel_pin: str, limit: int = 50):
    """Returns parcel owner name and address information, parcel information, and valuation based on Euclidean coordinate distance from the parcel pin."""
    query = f"""
    WITH eref AS
        (SELECT DISTINCT prpzip5 as zip, pin,
        (AVG(x_coord) OVER (PARTITION BY pin))::BIGINT AS x_coord,
        (AVG(y_coord) OVER (PARTITION BY pin))::BIGINT AS y_coord
        FROM {schema}.{parcels}
        WHERE pin = %(pin)s)
    SELECT DISTINCT objectid, p.pin, p.x_coord, p.y_coord,
        ownnam AS primary_owner, ownnam2 AS secondary_owner, ownnam3 AS tertiary_owner,
        prpaddress AS property_address,
        prpctynam AS property_city, prpstenam AS property_state, prpzip5 AS property_zip, totactval AS primary_market_value,
        mailstrnbr || ' ' || COALESCE(mailstrdir || ' ' || mailstrnam, mailstrnam) || ' ' || COALESCE(mailstrtyp || ' ' ||mailstrsfx || ' ' || mailstrunt, COALESCE(mailstrtyp || ' ' || mailstrsfx, mailstrtyp)) AS mailing_address,
        mailctynam AS mailing_city, mailstenam AS mailing_state, mailzip5 AS mailing_zip,
        {schema}.euclidean(p.x_coord, eref.x_coord, p.y_coord, eref.y_coord) AS euclidean_distance
    FROM {schema}.{parcels} AS p
    INNER JOIN eref ON eref.zip = p.prpzip5
    WHERE pindesc = '1' AND p.pin <> eref.pin
    ORDER BY euclidean_distance LIMIT %(limit)s
    """
    return pd.read_sql(query, engine, params={'pin': parcel_pin, 'limit': limit})

def neighbors_address(engine: Engine, address: str, city: str, limit: int = 50):
    """Returns parcel owner name and address information, parcel information, and valuation based on Euclidean coordinate distance from the given address in a city.
    Results are only as good as the address given (addresses for condos may return interesting neighbor results.)"""
    address_formatted = address.upper()
    replacements = {"%20": " ", "COURT": "CT", "STREET": "ST", "BOULEVARD": "BLVD", "DRIVE": "DR", "ROAD": "RD"}
    for old, new in replacements.items():
        address_formatted = address_formatted.replace(old, new)

    city_formatted = city.upper()
    query = f"""
    WITH eref AS
        (SELECT DISTINCT prpaddress AS property_address,
        pin,
        prpctynam AS city,
        (AVG(x_coord) OVER (PARTITION BY pin))::BIGINT AS x_coord,
        (AVG(y_coord) OVER (PARTITION BY pin))::BIGINT AS y_coord
        FROM {schema}.{parcels}
        WHERE prpaddress = %(address)s
        AND prpctynam = %(city)s)
    SELECT DISTINCT objectid, p.pin, p.x_coord, p.y_coord,
    ownnam AS primary_owner, ownnam2 AS secondary_owner, ownnam3 AS tertiary_owner,
    prpaddress AS property_address,
    prpctynam AS property_city, prpstenam AS property_state, prpzip5 AS property_zip, totactval AS primary_market_value,
    mailstrnbr || ' ' || COALESCE(mailstrdir || ' ' || mailstrnam, mailstrnam) || ' ' || COALESCE(mailstrtyp || ' ' ||mailstrsfx || ' ' || mailstrunt, COALESCE(mailstrtyp || ' ' || mailstrsfx, mailstrtyp)) AS mailing_address,
    mailctynam AS mailing_city, mailstenam AS mailing_state, mailzip5 AS mailing_zip,
    {schema}.euclidean(p.x_coord, eref.x_coord, p.y_coord, eref.y_coord) AS euclidean_distance
    FROM {schema}.{parcels} AS p
    INNER JOIN eref ON eref.city = p.prpctynam
    WHERE pindesc = '1' AND p.prpaddress <> eref.property_address
    ORDER BY euclidean_distance LIMIT %(limit)s;
    """
    return pd.read_sql(query, engine, params={'address': address_formatted, 'city': city_formatted, 'limit': limit})

# Endpoint for neighborhood turnover
def turnover_neighborhood(engine: Engine, years: int = 10):
    query = f"""
    WITH sales AS (
        SELECT PIN, NHDNAM, TO_DATE(SLSDT, 'MMDDYYYY') AS sale_date FROM {schema}.{parcels}
        UNION ALL
        SELECT PIN, NHDNAM, TO_DATE(SLSDT2, 'MMDDYYYY') FROM {schema}.{parcels}
        UNION ALL
        SELECT PIN, NHDNAM, TO_DATE(SLSDT3, 'MMDDYYYY') FROM {schema}.{parcels}
        UNION ALL
        SELECT PIN, NHDNAM, TO_DATE(SLSDT4, 'MMDDYYYY') FROM {schema}.{parcels}
    ),
    recent_sales AS (
        SELECT DISTINCT PIN, NHDNAM
        FROM sales
        WHERE sale_date >= CURRENT_DATE - INTERVAL %s
    ),
    neighbors AS (
        SELECT NHDNAM, COUNT(DISTINCT PIN) AS total_properties
        FROM {schema}.{parcels}
        GROUP BY NHDNAM
    )
    SELECT
        n.NHDNAM AS neighborhood,
        COUNT(rs.PIN) AS properties_sold_last_period,
        n.total_properties,
        ROUND(
            COUNT(rs.PIN)::numeric / NULLIF(n.total_properties, 0) * 100,
            2
        ) AS turnover_percent
    FROM neighbors n
    LEFT JOIN recent_sales rs USING (NHDNAM)
    GROUP BY n.NHDNAM, n.total_properties
    ORDER BY turnover_percent DESC;
    """
    return pd.read_sql(query, engine, params=(f"{years} years",))


# Endpoint for subdivision turnover
def turnover_subdivision(engine: Engine, years: int = 10):
    query = f"""
    WITH sales AS (
        SELECT PIN, SUBNAM, TO_DATE(SLSDT, 'MMDDYYYY') AS sale_date
        FROM {schema}.{parcels} WHERE TAXCLS LIKE '1%%'
        UNION ALL
        SELECT PIN, SUBNAM, TO_DATE(SLSDT2, 'MMDDYYYY')
        FROM {schema}.{parcels} WHERE TAXCLS LIKE '1%%'
        UNION ALL
        SELECT PIN, SUBNAM, TO_DATE(SLSDT3, 'MMDDYYYY')
        FROM {schema}.{parcels} WHERE TAXCLS LIKE '1%%'
        UNION ALL
        SELECT PIN, SUBNAM, TO_DATE(SLSDT4, 'MMDDYYYY')
        FROM {schema}.{parcels} WHERE TAXCLS LIKE '1%%'
    ),
    recent_sales AS (
        SELECT DISTINCT PIN, SUBNAM
        FROM sales
        WHERE sale_date >= CURRENT_DATE - INTERVAL %s
    ),
    subdivisions AS (
        SELECT SUBNAM, COUNT(DISTINCT PIN) AS total_properties
        FROM {schema}.{parcels}
        WHERE TAXCLS LIKE '1%%'
        GROUP BY SUBNAM
    )
    SELECT
        s.SUBNAM AS subdivision,
        COUNT(rs.PIN) AS properties_sold_last_period,
        s.total_properties,
        ROUND(
            COUNT(rs.PIN)::numeric / NULLIF(s.total_properties, 0) * 100,
            2
        ) AS turnover_percent
    FROM subdivisions s
    LEFT JOIN recent_sales rs USING (SUBNAM)
    GROUP BY s.SUBNAM, s.total_properties
    HAVING s.total_properties >= 20
    ORDER BY turnover_percent DESC;
    """
    interval = f"'{years} years'"
    return pd.read_sql(query, engine, params=(f"{years} years",))

# Endpoint for neighborhood value change
def value_change_by_neighborhood(engine: Engine):
    query = f"""
    SELECT
        NHDNAM AS neighborhood,
        SUM(TOTACTVAL::numeric) AS total_current_value,
        SUM(PYRTOTVAL::numeric) AS total_prior_value,
        SUM(TOTACTVAL::numeric) - SUM(PYRTOTVAL::numeric) AS value_change,
        ROUND(
            (SUM(TOTACTVAL::numeric) - SUM(PYRTOTVAL::numeric))
            / NULLIF(SUM(PYRTOTVAL::numeric), 0)::numeric * 100,
            2
        ) AS value_change_pct
    FROM {schema}.{parcels}
        WHERE TAXCLS LIKE '1%%'
            AND TOTACTVAL IS NOT NULL
            AND PYRTOTVAL IS NOT NULL
    GROUP BY NHDNAM
    HAVING SUM(PYRTOTVAL::numeric) > 0
    ORDER BY value_change_pct DESC;
    """
    return pd.read_sql(query, engine)

#testing username retrieval
def current_username(engine: Engine):
    df = pd.read_sql_query("SELECT CURRENT_USER AS username;", engine)
    return str(df.iloc[0]["username"])

# Add starred parcels to lookup table based on user name logged into engine and parcel objectid
def add_parcel(engine: Engine, object_id: str):
    df = pd.DataFrame({"username": [current_username(engine)], "objectid": [object_id]})
    return  df.to_sql(stars, engine, if_exists='append', index=False, schema=schema)


# Endpoint for modifying mailing addresses for parcels
def update_mailing_address(
    engine: Engine,
    parcel_pin: str,
    address_num: str,
    address_dir: str | None,
    address_name: str,
    address_type: str,
    address_suffix: str | None,
    city: str,
    state: str,
    zipcode5: str,
    zipcode4: str | None,
):
    sql = f"""
        UPDATE {schema}.{parcels}
        SET
            mailstrnbr = :address_num,
            mailstrdir = :address_dir,
            mailstrnam = :address_name,
            mailstrtyp = :address_type,
            mailstrsfx = :address_suffix,
            mailctynam = :city,
            mailstenam = :state,
            mailzip5   = :zipcode5,
            mailzip4   = :zipcode4
        WHERE pin = :parcel_pin;
    """

    params = {
        "parcel_pin": parcel_pin,
        "address_num": address_num,
        "address_dir": address_dir,
        "address_name": address_name,
        "address_type": address_type,
        "address_suffix": address_suffix,
        "city": city,
        "state": state,
        "zipcode5": zipcode5,
        "zipcode4": zipcode4,
    }

    with engine.begin() as conn:
        res = conn.execute(text(sql), params)
        return {"ok": True, "rows_affected": int(res.rowcount or 0)}

def main():
    login = input("Login username: ")
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    engine = create_engine(f'postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403')

    results = address_by_name(engine, 'mcdonald')
    print(results)
    
if __name__ == "__main__":
    main()
