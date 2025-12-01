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

def main():
    login = input("Login username: ")
    secret = parse.quote(str(os.getenv("DB_PASSWORD")))
    engine = create_engine(f'postgresql+psycopg2://{login}:{secret}@ada.mines.edu:5432/csci403')

    results = address_by_name(engine, 'mcdonald')
    print(results)
    
if __name__ == "__main__":
    main()
