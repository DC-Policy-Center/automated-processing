import pandas as pd
import os


def get_tract_from_fips(FIPS):
    try:
        FIPS_to_parse = str(FIPS)
        #t='{:f}'.format(float(FIPS_to_parse))

        ff = '{:f}'.format(FIPS)
        FIPS = str(ff)
        tract_to_fill = [item for item in FIPS]
        tract_to_format = ''.join(tract_to_fill[5:11])
        tract = '{0:.2f}'.format(int(tract_to_format)/100)
        #tract = tract.zfill(6)
    except:
        tract = 'err-tract-to-fips'
    return(tract)


data_folder_write = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")



file_to_open = os.path.join(data_folder_write, "NETS2015_general_with_FIPS_7.csv")
file_to_write = os.path.join(data_folder_write, "NETS2015_general_with_tract_2.csv")
print('loading')
df = pd.read_csv(file_to_open,encoding="ISO-8859-1")
print('loaded')
#df['tract'] = ''


#current_location_in_full_df = 0

#print('starting loop')

'''
while current_location_in_full_df <= len(df)-1:
    if current_location_in_full_df % 50000 == 0:
        print('Currently at index {} of {}'.format(current_location_in_full_df,len(df)))

    FIPS = df['FIPS'][current_location_in_full_df]
    tract = get_tract_from_fips(FIPS)
    df.ix[current_location_in_full_df,'tract'] = tract
    current_location_in_full_df += 1
'''
print('mapping')
df["tract"] = df["FIPS"].map(get_tract_from_fips)
print('mapped, \nwritting')

df.to_csv(file_to_write,encoding="ISO-8859-1")
print('done')
