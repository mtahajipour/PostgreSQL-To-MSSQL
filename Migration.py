import psycopg2
import pyodbc



def ConvertDataType (postgresql_datatype):
    if postgresql_datatype=='BYTEA':
        return 'BINARY(MAX)'
    elif postgresql_datatype=='boolean':
        return 'BIT'
    elif postgresql_datatype=='TEXT':
        return 'NVARCHAR(MAX)'
    elif postgresql_datatype=='VARCHAR':
        return 'NVARCHAR(MAX)'
    elif postgresql_datatype=='TIMESTAMP':
        return 'DATETIME'
    elif postgresql_datatype=='SMALLINT':
        return 'TINYINT'
    elif postgresql_datatype=='CHAR':
        return 'UNIQUEIDENTIFIER'
    elif postgresql_datatype=='MONEY':
        return 'SMALLMONEY'
    elif postgresql_datatype=='BIGINT':
        return 'BIGINT'
    elif postgresql_datatype=='DOUBLE PRECISION':
        return 'DOUBLE PRECISION'
    elif postgresql_datatype=='INTEGER':
        return 'INTEGER'
    elif postgresql_datatype=='DATE':
        return 'DATE'
    else:
        return 'NVARCHAR(MAX)'


server = 'localhost'
database = 'SAAMAP'
username = 'sa'
password = '1'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 11 for SQL Server};SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

conn=psycopg2.connect(host="192.168.100.121",database="SAAMAP", user="saamap", password="123456a!")
cur=conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='public';")
tables_name=cur.fetchall()


for table in tables_name:
        create_table=" create table  ["+table[0]+"] ( "
        cur.execute("select column_name,data_type from information_schema.columns where table_name = '%s' " %table[0])
        tables_info=cur.fetchall()
        insert_params='('
        for info in tables_info:
            insert_params=insert_params+'?,'
            field_name="["+info[0]+"]"
            field_type=ConvertDataType(info[1])
            create_table=create_table+field_name+" "+field_type+", "
        
        insert_params=insert_params[:-1]
        insert_params=insert_params+')'
        
        create_table=create_table[:-2]
        create_table=create_table+" );"
        
        cursor.execute(create_table)
        cnxn.commit()
        print(table[0],'Create Table Done!')
        cur.execute('select * from "public"."'+table[0]+'"')
        data=cur.fetchall()
        for row in data:
            try:
                insert_q='insert into ['+table[0]+'] values'+insert_params
                cursor.execute(insert_q,row)
                cnxn.commit()
            except Exception:
                print(table[0],'Inserting Data Failed!')
                
cnxn.close()
conn.close()



    

    
    



