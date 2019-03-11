import pandas as pd


def get_tract_from_fips(FIPS):
    tract = FIPS[5:10]
    return(tract)



data_folder_write = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")



file_to_open = os.path.join(data_folder_write, "NETS2015_general_with_FIPS.csv")
file_to_write = os.path.join(data_folder_write, "NETS2015_general_with_tract.csv")

df = pd.read_csv(file_to_open,encoding="ISO-8859-1")

df['tract'] = ''

for i in range(len(df)):
    FIPS = df['FIPS'][i]
    tract = get_tract_from_fips(FIPS)
    df['tract'][i] = tract


df.to_csv(file_to_write,encoding="ISO-8859-1")
