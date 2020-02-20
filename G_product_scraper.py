#!/usr/bin/env python
# coding: utf-8

import requests
import lxml
from lxml import html
import pandas as pd
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing as mp
import os
from datetime import datetime
from ast import literal_eval

# """____for input and output path____"""

root_path = os.path.dirname(os.path.abspath(__file__))
# root_path = os.getcwd()
input_path = os.path.join(root_path, "input")
output_path = os.path.join(root_path, "output")

#____initializing date & time for output file name
today = datetime.now().strftime('%m%d_%H%M')


# """ ____importing dictionary from xpath.csv____"""
dic_file = pd.read_csv(os.path.join(input_path, 'xpath.csv'),
                                    delimiter="\t")
dic_file.dropna(inplace = True) 
d_x = dic_file.to_dict(orient='dict')

field =d_x['field']
xpath = d_x['xpath']
d_xpath ={}
for k in range(len(field)):
    d_xpath[field[k]] = xpath[k]

#____importing url from url.csv____
list_url = pd.read_csv(os.path.join(input_path, 'url.csv'),
                                    delimiter="\t")['url'].values


# ____request for url____
def scrap_url(list_of_url, dict_of_xpath):
    session_requests = requests.session()
    new = {}
    v_list = []
    for url in list_of_url:
        try:
            print(url+"\n")
            conv_list = []
            result = session_requests.get(url)
            tree = html.fromstring(result.content)
            new['url'] = url
            for key in [*dict_of_xpath]:
                data = list(set(tree.xpath(dict_of_xpath[key])))
                new[key] = data
                
            for key in [*new]:
                conv_list.append(new[key])
            
            v_list.append(conv_list)

        except:
            print(url)
            for key in [*dict_of_xpath]:
                data = "error scrapping data"
                
            for key in [*new]:
                conv_list.append(new[key])

            v_list.append(conv_list)
        
    scrap_df = pd.DataFrame(v_list, columns=[*new])
    return scrap_df

"""____to create variation from option list with recursive function____"""
"""____ function takes current column from which you have to check if data is in list,
         totalcol as total number of columns and dataframe as dataframe for which - function will determine data is whether in list.
         [Note: You can also apply function to specific column only by giving that column index(in number) and total column as 'column index + 1']"""

def rec_if_clist(currentcol,totalcol,dataframe):
    if currentcol == totalcol:
        return dataframe
    
    else:
        col = currentcol
        cols = [*dataframe.columns]
        all_df = pd.DataFrame(columns=[*dataframe.columns])
        for d_index in range(len([*dataframe.index])):
            print("Processing row {}".format(d_index))
            ref_row = dataframe.iloc[d_index]
#             data1 = literal_eval(ref_row[col])  #______when using function to dataframe from csv file.
            data1 = ref_row[col]
            if str(type(data1)) == "<class 'list'>":
                for d in range(len(data1)):
                    n_dic = {}
                    for r_data in range(len([*ref_row.values])):
                        if r_data == col:
                            n_dic[cols[r_data]] = data1[d]

                        else:
                            n_dic[cols[r_data]] = ref_row.iloc[r_data]

                    add_df = pd.DataFrame([n_dic], columns=[*dataframe.columns]) 
                    all_df = all_df.append(add_df, ignore_index = True)

            else:
                print("Recurssion called for column: {}".format(currentcol+1))
                all_df = all_df.append(ref_row, ignore_index = True)

        return rec_if_clist(currentcol+1,totalcol,all_df)

#____Preparing arguments for multiprocessing for scraping url
print("Note:  Decrease process_count if scraping with multiprocessing will block ip")
arg_for_url_scrap = []
process_count = mp.cpu_count()
list_split = np.array_split(list_url, process_count)
for i in range(process_count):
    valu = (list_split[i],d_xpath)
    arg_for_url_scrap.append(valu)


#____calling 'scrape_url' function using multiprocessing and assigning returned list to 'general_data' 
pool = ThreadPool(process_count)
general_data = pool.starmap(scrap_url, arg_for_url_scrap)

#____initializing empity dataframe to append list data to dataframe
data = pd.DataFrame( columns=[['url']+ [*d_xpath.keys()]][0])

#____converting list data to dataframe
for i in general_data:
    data = data.append(i, ignore_index = True)
#____writing 'data' to csv file
data.to_csv(os.path.join(output_path, "general_data_"+today+".csv"), sep='\t', encoding='utf-8', index=False)  #____basic output without variation

#____Preparing arguments for multiprocessing for creating variation with rec_if_clist
arg_for_recurssion = []
process_count1 = mp.cpu_count()
df_split = np.array_split(data, process_count1)
for i in range(process_count1):
    valu = (0,len(df_split[i].columns),df_split[i])
    arg_for_recurssion.append(valu)

#____calling 'scrape_url' function using multiprocessing and assigning returned list to 'general_data' 
pool1 = ThreadPool(process_count1)
n_data = pool1.starmap(rec_if_clist, arg_for_recurssion)

#____initializing empity dataframe to append list data to dataframe
new_data = pd.DataFrame( columns=[['url']+ [*d_xpath.keys()]][0])
#____converting list data to dataframe
for i in n_data:
    new_data = new_data.append(i, ignore_index = True)

#____writing 'new_data' to csv file
new_data.to_csv(os.path.join(output_path, "data_with_variation_"+today+".csv"), sep='\t', encoding='utf-8', index=False) #____output with variation

