import logging
import azure.functions as func

import pandas as pd
import unittest
import xmlrunner
import datacompy
import pyodbc

import logging

import azure.functions as func


server = r'M5-L-F2AJJ94'
database = 'TestDB'
username = 'sa'
password = 'Rashmi123'



# class TestDemo(unittest.TestCase):
    
def compareSqldata(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # cnxn = pyodbc.connect(
    # 'DRIVER={ODBC Driver 17 for SQL Server}; \
    # SERVER='+ server +'; \
    # DATABASE='+ database +';\
    # Trusted_Connection=no;')


    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    query1="SELECT * FROM tbltest_info"
    query2="SELECT * FROM tbltest_info"
    df1 = pd.read_sql(query1, cnxn)
    df2 = pd.read_sql(query2, cnxn)
    # print(df1)
    # print(df2)
    cursor.execute(query1)
    cursor.execute(query2)
    result=cursor.fetchall()
    print(result)

    cnxn.commit()
    cursor.close()

        # src_df = pd.read_csv(r'src.csv')
        # dest_df = pd.read_csv(r'dest.csv')
        
        # print (src_df)
        
    compare = datacompy.Compare(df1,df2,
    join_columns='UNAME',  #You can also specify a list of columns
    abs_tol=0, #Optional, defaults to 0
    rel_tol=0, #Optional, defaults to 0
    df1_name='Original', #Optional, defaults to 'df1'
    df2_name='New' #Optional, defaults to 'df2'
    )

        # display unmatched rows
    df_diff = pd.concat([df1,df2]).drop_duplicates(keep=False)
    print(df_diff)

        # cursor1 = cnxn.cursor()
        # for index, row in df_diff.iterrows():
        #     cursor1.execute("INSERT INTO dbo.tbl_ComparisonResult ([id],[name],[age],[location],[salary]) values(?,?,?,?,?)", row['id'], row['name'], row['age'],row['location'],row['salary'])

    print(compare.report())

    return func.HttpResponse(compare.report())
    # def test_TargetTableData_NonUnicode_dest1_to_TargetTableData_NonUnicode_dest2_data(self):

    #     src_df = pd.read_json(r'TargetTableData_NonUnicode_dest1.json')
    #     dest_df = pd.read_json(r'TargetTableData_NonUnicode_dest2.json')
        
    #     #print (src_df)
        
    #     compare = datacompy.Compare(src_df,dest_df,
    #     join_columns='OrgUnitNbr',  #You can also specify a list of columns
    #     abs_tol=0, #Optional, defaults to 0
    #     rel_tol=0, #Optional, defaults to 0
    #     df1_name='Destination1', #Optional, defaults to 'df1'
    #     df2_name='Destination2' #Optional, defaults to 'df2'
    #     )

    #     print(compare.report())

        
# if __name__ == '__main__':
#     # print("main")
#         unittest.main(
#         testRunner=xmlrunner.XMLTestRunner(output='test-reports'),
#         failfast=False, buffer=False, catchbreak=False)
