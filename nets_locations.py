import os
import pandas as pd
import grequests
import requests


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
    resp = requests.get(url)
    FIPS = resp.json()['Block']['FIPS']
    return(FIPS)


# File locations
data_folder_write = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")

file_to_open = os.path.join(data_folder_read, "NETS2015_general_with_FIPS_6.csv")
file_to_write_partial = os.path.join(data_folder_write, "NETS2015_general_with_FIPS_partial_7.csv")
file_to_write = os.path.join(data_folder_write, "NETS2015_general_with_FIPS_7.csv")

# reading in dataframe from CSV
print('opening')
df = pd.read_csv(file_to_open,encoding="ISO-8859-1")

master_fips_list = []

batch_len = 1 # A batch refers to one set of URLs to be pulled at once


current_location_in_full_df = 1900000

print('starting loop')
num_left = len(df)

while current_location_in_full_df <= len(df)-1:
    fips_list = []
    if current_location_in_full_df % 50000 == 0:
        print('Currently at index {} of {}'.format(current_location_in_full_df,len(df)))

    if num_left < batch_len:
        batch_len = num_left

        '''
    for batch_index in range(batch_len):
        url_list = []
        lat = df['Latitude'][current_location_in_full_df + batch_index]
        lon = df['Longitude'][current_location_in_full_df + batch_index]
        url = url_builder(lat,lon)
        url_list.append(url)
        '''
    lat = df['Latitude'][current_location_in_full_df]
    lon = df['Longitude'][current_location_in_full_df]
    FIPS = get_FIPS(lat,lon)
    master_fips_list.append(FIPS)
    #df['FIPS'][current_location_in_full_df] = FIPS
    df.ix[current_location_in_full_df,'FIPS'] = FIPS
    #fips_list = get_batch_FIPS(url_list)
    #for item in fips_list: master_fips_list.append(item)

    '''
    ii = current_location_in_full_df
    for ii in range(len(master_fips_list)):
        df['FIPS'][ii] = master_fips_list[ii]
        ii += 1
        '''
    #print('incrementing')

    current_location_in_full_df += batch_len
    if current_location_in_full_df % 100000 == 0:
        print('Writing partial')
        df.to_csv(file_to_write_partial,encoding="ISO-8859-1")

    num_left += current_location_in_full_df

print('writing file')
df.to_csv(file_to_write,encoding="ISO-8859-1")
