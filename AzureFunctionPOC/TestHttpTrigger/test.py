import pandas as pd
import unittest
import xmlrunner
import datacompy
import time
import datetime

import logging
import json
import azure.functions as func

def CompareCsv(req: func.HttpRequest) -> func.HttpResponse:

    print("Start of Script:-", datetime.datetime.now())
    # src_df = pd.read_csv(r'C:\AzureFunctionPOC\TestHttpTrigger\src.csv')
    # dest_df = pd.read_csv(r'C:\AzureFunctionPOC\TestHttpTrigger\dest.csv')

    # 40 records
    src = 'https://comparefilesfuncapp.blob.core.windows.net/csvblob/src.csv?sp=r&st=2020-11-02T05:35:44Z&se=2020-11-06T18:29:44Z&spr=https&sv=2019-12-12&sr=b&sig=ZZfxIjTWTZ0AbHWnUFL99KBcb9eZak82XoJk1nonY08%3D'
    dest = 'https://comparefilesfuncapp.blob.core.windows.net/csvblob/dest.csv?sp=r&st=2020-11-02T05:36:43Z&se=2020-11-06T18:29:43Z&spr=https&sv=2019-12-12&sr=b&sig=TnzlvKculK%2FWc4T3wEo3NvpJp4ytPiCvg9fAI0nHbGs%3D'
    # 5M records
    # src = 'https://comparefilesfuncapp.blob.core.windows.net/csvblob/csvoutput5MSrc.csv?sp=r&st=2020-11-03T06:59:59Z&se=2020-11-06T17:59:59Z&spr=https&sv=2019-12-12&sr=b&sig=BYhS66f063Lg6HIgYQbqUUOt4SuwYXEXocFBXQ%2BscbU%3D'
    # dest = 'https://comparefilesfuncapp.blob.core.windows.net/csvblob/csvoutput5MDest.csv?sp=r&st=2020-11-03T07:00:31Z&se=2020-11-06T18:00:31Z&spr=https&sv=2019-12-12&sr=b&sig=JcvWMFHFh%2BSVgM8AyIHviZCeGyOMekL22ZvzhwrhVLI%3D'

    src_df = pd.read_csv(src)
    dest_df = pd.read_csv(dest)
        
    compare = datacompy.Compare(src_df,dest_df,
    on_index = True,
    abs_tol=0, #Optional, defaults to 0
    rel_tol=0, #Optional, defaults to 0
    df1_name='Original', #Optional, defaults to 'df1'
    df2_name='New' #Optional, defaults to 'df2'
    )

    print("End of Script:-", datetime.datetime.now())
    return func.HttpResponse(compare.report())


