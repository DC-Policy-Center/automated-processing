import time
tic = time.clock()


import requests
import pandas as pd
import grequests
import os


data_folder = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
log_to_write = os.path.join(data_folder, "log.txt")
verbose= False

def write_log(mes,verbose):
    with open(log_to_write,'a') as f:
        f.write("\n%s"%mes)
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


def get_FIPS(lat,lon):
    fips_list = []
    url = "https://geo.fcc.gov/api/census/block/find?latitude={}&longitude=-{}&showall=false&format=json".format(str(lat),str(lon)) #NEGATIVE BUILT INTO URL BUILDER FOR LONGITUDE
    FIPS = resp.json()['Block']['FIPS']
    return(FIPS)


def get_tract_from_fips(FIPS):
    tract = FIPS[5:10]
    return(tract)





write_log("loading in data",verbose)
#C:\Users\mwatson\Documents\SharePoint\The DC Policy Center - Documents\Data\NETS Data\NETS Subsets
data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets")
file_to_open = os.path.join(data_folder_read, "NETS2015_general.csv")
file_to_write = os.path.join(data_folder, "NETS2015_general_with_tract.csv")


df = pd.read_csv(file_to_open,encoding="ISO-8859-1")
df['FIPS'] = ''
write_log("finished loading",verbose)






i = 0

df_len = len(df)
run_len = df_len

batch_len = 1000

master_fips_list = []


for i in range(run_len):
    fips_list = []

    line = "{} batch starting -- Index: {} out of {}\n".format(str(i+batch_len),str(i),str(run_len))
    write_log(line,verbose)

# ---  building URL batch ---
    for j in range(batch_len):
        url_list = []
        lat = df['Latitude'][i]
        lon = df['Longitude'][i]
        url = url_builder(lat,lon)
        url_list.append(url)
# ---------------------------


# ---  sending batch list to get FIPS
    line = "getting fips for {} batch -- Index: {} out of {}\n".format(str(j),str(i),str(run_len))
    fips_list = get_batch_FIPS(url_list)
    write_log(line,verbose)

# -- appending fips to list
    line = "Appending -- {} batch completed -- Index: {} out of {}\n".format(str(j),str(i),str(run_len))
    for item in fips_list: master_fips_list.append(item)
    write_log(line,verbose)


    line = "{} batch completed -- Index: {} out of {}\n".format(str(j),str(i),str(run_len))
    write_log(line,verbose)

    i+=batch_len


ii = 0
for ii in range(len(master_fips_list)):
    df['FIPS'][ii] = master_fips_list[ii]


write_log("writing to file",verbose)
df.to_csv(file_to_write, sep=',')
write_log("finsihed writing to file",verbose)



toc = time.clock()
write_log("{} seconds".format(str(toc-tic)),verbose)
