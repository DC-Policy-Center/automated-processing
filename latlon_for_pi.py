

import time
tic = time.clock()

#------------------- Import statements
import requests
import pandas as pd
import grequests
import os

#------------------- Set up initial variables
#data_folder = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
#C:\Users\mwatson\Documents\SharePoint\The DC Policy Center - Documents\Data\NETS Data\NETS Subsets
#data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets")


mes = {'file_length':0,
       'batch_index':0
       }



data_folder_write = os.path.join("~", "Desktop","nets-explore","tract")
data_folder_read = os.path.join("~", "Desktop","nets-explore")

file_to_open = os.path.join(data_folder_read, "NETS2015_general.csv")
file_to_write = os.path.join(data_folder_write, "NETS2015_general_with_tract.csv")


log_to_write = os.path.join(data_folder_write, "log.html")
verbose= False

#-------------------  Define methods
def write_log(mes,verbose):
    file_length = mes['file_length']


    output_string = '''
                        <h1> </h1>
                        <h2> </h2>

    '''

    with open(log_to_write,'a') as f:
        f.write(output_string)
        f.close()
        if verbose:print(mes)

def url_builder(lat,lon):
    url = "https://geo.fcc.gov/api/census/block/find?latitude={}&longitude=-{}&showall=false&format=json".format(str(lat),str(lon)) #NEGATIVE BUILT INTO URL BUILDER FOR LONGITUDE
    return(url)

def get_batch_FIPS(ll_list):
    rs = [grequests.get(u) for u in url_list]
    grequests.map(rs)
    for item in range(len(rs)):
        fips_list.append(rs[item].response.json()['Block']['FIPS'])
    return(fips_list)

def write_part(df,step,path):
    file_name = "partial_nets_block_{}.csv".format(str(step))
    partial_to_write = os.path.join(path, file_name)
    df.to_csv(partial_to_write, sep=',')



#------------------- Load in data
write_log("loading in data",verbose)


df = pd.read_csv(file_to_open,encoding="ISO-8859-1")
df['FIPS'] = ''
write_log("finished loading",verbose)


# Batch is used as a small number of URLs to be built and called together
batch_len = 1000

df_len = len(df)
mes['file_length'] = df_len
write_log(mes,verbose)

run_len = 10000#df_len/100



master_fips_list = []
binned_fips_list = [] # A bin is the 10,000 that will be used as the partial output

bin_output_index = 0
bin_output_length = 10000


for full_index in range(10000) # This loop runs through the entire dataframe (~2mil)

    for batch_index in range(run_len): # this loop batches into the single request list (1000)
        i = batch_index
        fips_list = []

        #mes['batch_index'] = batch_index
        #write_log(mes,verbose)

    # ---  building URL batch ---
        for j in range(batch_len):
            url_list = []
            lat = df['Latitude'][i]
            lon = df['Longitude'][i]
            url = url_builder(lat,lon)
            url_list.append(url)
    # ---------------------------

    # ---  sending batch list to get FIPS
        fips_list = get_batch_FIPS(url_list)

    # -- appending fips to list
        for item in fips_list: master_fips_list.append(item)
        #for item in fips:list: binned_fips_list.append(item)


        partial_output_index +=1

        ''''
        if partial_output_index == partial_output_lenght:
            # Write partial file

            write_part('data_folder_write')

            partial_output_index = 0
            binned_fips_list = []
        ''''
        batch_index+=batch_len



ii = 0
for ii in range(len(master_fips_list)):
    df['FIPS'][ii] = master_fips_list[ii]


write_log("writing to file",verbose)
df.to_csv(file_to_write, sep=',')
write_log("finsihed writing to file",verbose)




####################################### ENDING TIME #########
toc = time.clock()
write_log("{} seconds".format(str(toc-tic)),verbose)        #
#############################################################
