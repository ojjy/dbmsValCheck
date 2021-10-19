from dbms.mysql_con import MYSQLdb
from dbms.sf_con import Snowflakedb
from dbms.postgres_con import Postgredb

import json
import pymssql
import pyodbc

if __name__ == "__main__":
    with open("dbinfo.json") as con:
        dbinfo = json.loads(con.read())
    print(dbinfo)
    # MYSQLdb.validate(user=dbinfo['MYSQL_USER'], pwd=dbinfo['MYSQL_PWD'], host=dbinfo['MYSQL_HOST'], port=dbinfo['MYSQL_PORT'], db=dbinfo['MYSQL_DB'])
    # Snowflakedb.validate(user=dbinfo['SF_USER'], pwd=dbinfo['SF_PWD'], account=dbinfo['SF_ACCOUNT'])
    # Postgredb.validate(user=dbinfo['POSTGRE_USER'], pwd=dbinfo['POSTGRE_PWD'], host=dbinfo['POSTGRE_HOST'], port=dbinfo['POSTGRE_PORT'], db=dbinfo['POSTGRE_DB'])
    con = pyodbc.connect(host=dbinfo['MSSQL_HOST'], user=dbinfo['MSSQL_USER'], password=dbinfo['MSSQL_PWD'], port=dbinfo['MSSQL_PORT'],
                    database=dbinfo['MSSQL_DB'])

    cur = con.cursor()
    cur.execute("SELECT @@version;")
    version = cur.fetchone()
    print(version)
