from dbms.mysql_con import MYSQLdb
from dbms.sf_con import Snowflakedb
from dbms.postgres_con import Postgredb

import json
import pymssql
import pyodbc
import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":
    with open("dbinfo.json") as con:
        dbinfo = json.loads(con.read())
    print(dbinfo)
    # MYSQLdb.validate(user=dbinfo['MYSQL_USER'], pwd=dbinfo['MYSQL_PWD'], host=dbinfo['MYSQL_HOST'], port=dbinfo['MYSQL_PORT'], db=dbinfo['MYSQL_DB'])
    # Snowflakedb.validate(user=dbinfo['SF_USER'], pwd=dbinfo['SF_PWD'], account=dbinfo['SF_ACCOUNT'])
    # Postgredb.validate(user=dbinfo['POSTGRE_USER'], pwd=dbinfo['POSTGRE_PWD'], host=dbinfo['POSTGRE_HOST'], port=dbinfo['POSTGRE_PORT'], db=dbinfo['POSTGRE_DB'])
    # con = pymssql.connect(user=dbinfo['MSSQL_USER'], password=dbinfo['MSSQL_PWD'], host=dbinfo['MSSQL_HOST'], port=dbinfo['MSSQL_PORT'], database=dbinfo['MSSQL_DB'])
    #
    # cur = con.cursor()
    # cur.execute("SELECT @@version;")
    # version = cur.fetchone()
    # print(version)

    data_list = ['DM_M_CC_MT_CD_003.csv', 'DM_M_CC_SICK_CD_001D.csv', 'DW_W_CC_MCI_FACT_001.csv', 'DW_W_CC_META_CD_001B.csv']
    for data in data_list:
        table_name = data[3:].split('.')[0]
        print(table_name)

        df = pd.read_csv(data, encoding='euc-kr')
        print(df.head(10))
        cnx = create_engine(
            f"mysql+pymysql://{dbinfo['MYSQL_USER']}:{dbinfo['MYSQL_PWD']}@{dbinfo['MYSQL_HOST']}/{dbinfo['MYSQL_DB']}?charset=utf8mb4")
        cnx.execute(f"TRUNCATE TABLE {table_name}")
        df.to_sql(name=f'{table_name}', con=cnx, index=False, if_exists='append')

    # for data in data_list:
    #     df = pd.read_csv(data, encoding='euc-kr')
    #     print(df.head(10))
    #     cnx = create_engine(f"mysql+pymysql://{dbinfo['MYSQL_USER']}:{dbinfo['MYSQL_PWD']}@{dbinfo['MYSQL_HOST']}/{dbinfo['MYSQL_DB']}?charset=utf8mb4")
    #     df.to_sql(name='M_CC_MT_CD_003', con=cnx, index=False, if_exists='replace')
