import time
import datetime
import pandas as pd
import datacompy

import logging
import azure.functions as func

def CompareFiles(src,destn,delimiter,encod):
    src_df = pd.read_csv(src,encoding=encod)
    dest_df = pd.read_csv(destn,encoding=encod)

    compare = datacompy.Compare(src_df,dest_df,
        join_columns=None,  #You can also specify a list of columns
        on_index=True,
        abs_tol=0, #Optional, defaults to 0
        rel_tol=0, #Optional, defaults to 0
        df1_name='Original', #Optional, defaults to 'df1'
        df2_name='New' #Optional, defaults to 'df2'
        )
    return compare.report()

def main(req: func.HttpRequest) -> func.HttpResponse:
    print("Start of Script:-", datetime.datetime.now())

    source = req.params.get('src')
    destn = req.params.get('dest')
    delimiter = req.params.get('delimit')
    encoding = req.params.get('encod')

    report = CompareFiles(source,destn,delimiter,encoding)

    print("End of Script:-", datetime.datetime.now())
    return func.HttpResponse(report)