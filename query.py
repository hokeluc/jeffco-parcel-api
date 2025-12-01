import pg8000
import getpass # temp

# these can be globals defined in another file
schema = 'kkubaska'
table = 'jeffco_staging'

def address_by_name(cursor: pg8000.Cursor, name: str):
    # should be updated, I think concating a null field makes the whole result null, so won't work for single owner homes
    query = "select " +\
        "ownnam || '|' || ownnam2 as owners, " +\
        "prpaddress || ', ' || prpctynam || ', ' || prpzip5 as address " +\
        f"from {schema}.{table} " +\
        "where ownnam ilike '%%' || %s || '%%' or ownnam2 ilike '%%' || %s || '%%';"
    print(query)
    cursor.execute(
        query,
        [name, name])
    return cursor.fetchall()


def main():
    login = input('Login username: ')
    secret = getpass.getpass()
    credentials = {'user' : login,
    'password': secret,
    'database': 'csci403',
    'host' : 'ada.mines.edu'}
    db = pg8000.connect(**credentials)
    cursor = db.cursor()

    results = address_by_name(cursor, 'mcdonald')
    for owners, address in results:
        print(owners, address, sep="    ")
    


if __name__ == "__main__":
    main()
