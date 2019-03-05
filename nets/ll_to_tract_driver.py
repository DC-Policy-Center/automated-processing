import time
tic = time.clock()



import censusgeocode as cg # https://github.com/fitnr/censusgeocode
import pandas as pd
import os.path

data_folder = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets","tract")
log_to_write = os.path.join(data_folder, "log.txt")
verbose= False

def write_log(mes,verbose):
    with open(log_to_write,'a') as f:
        f.write("\n%s"%mes)
        f.close()
        if verbose:print(mes)





def get_tract(lat,lon):
    try:
        ret = cg.coordinates(x=-lon, y=lat)
    except:
        write_log('failed',verbose)
        tract=0
    #print(ret)
    try:
        tract = int(ret['Census Tracts'][0]['TRACT'])
    except:
        tract=0
    #print(tract)
    return(tract)




write_log("loading in data",verbose)
#C:\Users\mwatson\Documents\SharePoint\The DC Policy Center - Documents\Data\NETS Data\NETS Subsets
data_folder_read = os.path.join("C:\\", "Users","mwatson","Documents","SharePoint","The DC Policy Center - Documents","Data","Nets Data","NETS Subsets")
file_to_open = os.path.join(data_folder_read, "NETS2015_general.csv")
file_to_write = os.path.join(data_folder, "NETS2015_general_with_tract.csv")
#log_to_write = os.path.join(data_folder, "log.txt")
failed_log_to_write = os.path.join(data_folder, "failed.txt")

df = pd.read_csv(file_to_open,encoding="ISO-8859-1")
df['lat_lon_to_tract'] = ''
write_log("finished loading",verbose)

i = 0
failed_index = []
df_len = len(df)
run_len = df_len
for i in range(run_len):
    if i%20000 == 0:
        perc = (i/run_len) * 100
        line = "{} percent completed -- Index: {} out of {}\n".format(str(perc),str(i),str(run_len))
        write_log(line,verbose)

    lat = df['Latitude'][i]
    lon = df['Longitude'][i]
    tract = get_tract(lat,lon)
    if tract == 0:
        failed_index.append(i)

    df['lat_lon_to_tract'][i] = tract
    i+=1

write_log("writing to file",verbose)
df.to_csv(file_to_write, sep=',')
write_log("finsihed writing to file",verbose)


with open(failed_log_to_write,'w') as f:
    f.write('')
    f.close()
with open(failed_log_to_write,'a') as f:
    for c in failed_index:
        line = "%s\n"%str(c)
        f.write(line)
    f.close()


toc = time.clock()
write_log("{} seconds".format(str(toc-tic)),verbose)
