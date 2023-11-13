# -*- coding: utf-8 -*-
"""
Created October 2021

@author: BIM Docent
"""
import pandas as pd
import numpy as np
import sys, sqlalchemy, requests, csv, random
from datetime import timedelta, datetime
from urllib.parse import quote_plus
#---FUNCTIES------------------------------------------------
def ConnectDB():
#Deze functie zorgt voor de verbinding van python met de lokale mamp database 
#(local host: 127.0.0.1) op de computer. Indien er een goede verbinding kan
#worden gemaakt wordt de boodschap success teruggegeven en anders een 
#foutmelding. Let op pymysql moet wel zijn geinstalleerd in de python omgeving
#die wordt gebruikt, In ons geval anaconda!
    try:
        
        db_connector = 'mysql+pymysql://'
        db_username = quote_plus('username')
        db_sep1    = ":"
        db_password = quote_plus("password")
        db_sep2     = '@'
        db_server   = 'mysql.database.azure.com'
        db_sep3     = '/'
        db_name     = 'webshop'
        engine_name = \
            db_connector + \
            db_username + \
            db_sep1 + \
            db_password + \
            db_sep2 + \
            db_server + \
            db_sep3 + \
            db_name
        #inlogstring = "mysql+pymysql://%s:%s@bimmysql.mysql.database.azure.com/meubels" \
        #                % (quote_plus("bimadmin"),quote_plus("bjrbjr@1959"))
        #print(inlogstring)
        #print(engine_name)
        #sys.exit()
        engine = sqlalchemy.create_engine(engine_name)  
                           
        print
        #engine = sqlalchemy.create_engine(engine_name,  execution_options={"isolation_level": "AUTOCOMMIT"})
        #engine = OpenDB()
        con = engine.connect()
        return con, 'success'
    except Exception as e:
        return '', e

def DfToDb(con, df, table):
#Deze functie zorgt voor het inserten van een pandas dataframe in een MySQL DB
#als input wordt gegeven: 
#de database verbinding
#het dataframe
#de Db tabelnaam    
    try:
        df.to_sql(table, con, if_exists = 'append', index = False, chunksize = 1000)
        print('Insert in', table, 'database tabel succesvol')
    except Exception as e:
        print('Insert in', table, 'niet succesvol', e)
        print(type(e))
        sys.exit()

def QueryDB(con, table):
#Deze functie zorgt voor het uitvoeren van een SQL statement dat het aantal
#records in een tabel telt. Als input wordt de database verbiding en de tabel
#naam gegeven
    sql = 'SELECT COUNT(*) FROM '+table
    result = con.execute(sql).fetchone()
    print('Aantal records in webshop.'+table+':', result['COUNT(*)'])



#---HOOFDPROGRAMMA------------------------------------------

#-------Inlezen csv bestanden dataframe---
dfC = pd.read_csv('dim_customer.csv', delimiter=";", encoding="utf-8-sig", decimal = ',')
dfP = pd.read_csv('dim_product.csv', delimiter=";", encoding="utf-8-sig", decimal = ',')
dfWS = pd.read_csv('fact_sales.csv', delimiter=";", encoding="utf-8-sig", decimal = ',')
dfMt = pd.read_csv('dim_monthtarget.csv', delimiter=";", encoding="utf-8-sig", decimal = ',')
#---EINDE Inlezen-------------------------------


#---Database connect---------
con, e = ConnectDB()

if e == 'success':
    # VERWIJDER DATA IN DB TABELLEN-------------- 
    #Eerst worden alle tabellen in de DB geleegd
    con.execute("SET FOREIGN_KEY_CHECKS = 0")
    
    con.execute("TRUNCATE TABLE dim_customer")
    con.execute("TRUNCATE TABLE dim_monthtarget")
    con.execute("TRUNCATE TABLE fact_sales")
    con.execute("TRUNCATE TABLE dim_product")
    
    # VUL DB TABELLEN MET DATAFRAMES
    print("-------------------")    
    DfToDb(con, dfC, 'dim_customer')
    DfToDb(con, dfMt, 'dim_monthtarget')
    DfToDb(con, dfWS, 'fact_sales') 
    DfToDb(con, dfP, 'dim_product')
    
    # TEL RECORDS IN DB TABELLEN
    print("-------------------")
    QueryDB(con, 'dim_customer')
    QueryDB(con, 'dim_monthtarget')
    QueryDB(con, 'fact_sales')
    QueryDB(con, 'dim_product')
    
    con.execute("SET FOREIGN_KEY_CHECKS = 1")
    con.close()

else:
    print('database error', e)
#---EINDE lOAD---------------------------------

#---EINDE HOOFDPORGRAMMA------------------------------------
