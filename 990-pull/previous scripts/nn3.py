import json
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

def xml_tag_selector(xml_soup,selector):
    found = xml_soup(selector)
    r = 'NULL'
    rl = []
    i = 0
    for item in range(len(found)):
        try:
            for node in found[i]:
                r = ''.join(node)
                rl.append(r)
        except:
            r = 'NULL'
            rl.append(r)
        if r == '\n':
            r = 'EMPTY-NULL'
            rl.append(r)
        i +=1
    return(rl)

def get_org(data,search_name,search_type = 'fuzzyname'):
    i = 0
    orgs = []
    if search_type == 'fuzzyname':
        for i in range(len(data)):
            org_name = data[i]['OrganizationName'].lower()
            url = data[i]['URL']
            #search_name = 'higher achievement'
            search_name = search_name.lower()
            if search_name in org_name:
                orgs.append([org_name,url])
            else: i+=1
    elif search_type == 'ein':
        for i in range(len(data)):
            org_ein = data[i]['EIN']
            org_name = data[i]['OrganizationName'].lower()
            url = data[i]['URL']
            #search_name = 'higher achievement'
            search_ein = search_name
            if search_ein == org_ein:
                orgs.append([org_name,url])
            else: i+=1
    return(orgs)

def nn_request(url):
    nn = requests.get(url)
    soup = bs(nn.content,'lxml')
    items = []
    items.append([tag.name for tag in soup.find_all()])
    items = items[0]
    nn = {'soup':soup,
       'headers':items
       }
    return(nn)

def rename_dups(df):
    dup = 0
    cols = 0
    d_idx = 0
    cols=pd.Series(df.columns)
    for dup in df.columns.get_duplicates():
        cols[df.columns.get_loc(dup)]=[dup+'.'+str(d_idx) if d_idx!=0 else dup for d_idx in range(df.columns.get_loc(dup).sum())]
    df.columns=cols
    return(df)

def df_header_combine(dfaa,dfbb):
    keys_a = []
    keys_b = []
    keys_ab = []
    keys_ba = []
    dfa = rename_dups(dfaa)
    dfb = rename_dups(dfbb)

    for key in dfa.keys(): keys_a.append(key)
    for key in dfb.keys(): keys_b.append(key)
    for item in keys_a:
        if item not in keys_b: keys_ab.append(item)
    for item in keys_b:
        if item not in keys_a: keys_ba.append(item)

    for item in keys_ab: dfb[item] = 'HEADERFILL-NULL'
    for item in keys_ba: dfa[item] = 'HEADERFILL-NULL'

    dfc = dfa.append(dfb,verify_integrity=False)

    return(dfc)
def org_to_final_df(org,df_final):
    url = org[0][1]                        # saves url from org return
    found_single.append(org)
    nn = nn_request(url)            # retrieves 990 info from aws/irs
  # breaking out dict return from the 990
    soup = nn['soup']
    headers = nn['headers']
    values = []
    unique_headers = []
    #print('dropping ' + str(org_list.index[counting_index]))
    org_dropped_list.append(org_to_search)

    for header in headers:
        if header not in unique_headers: unique_headers.append(header)
    for uh in unique_headers:
        values.append(xml_tag_selector(soup,uh))

    df = pd.DataFrame(columns = unique_headers)
    df.loc[0] = values


    df['AAA Organization Name'] = org_to_search
    df['AAA Year Found'] = str(year)

    if df_final.empty:
        df_final = df
    else:
        df_final = df_header_combine(df_final,df)
    return(df,df_final)

def write_select_key_dict_pairs(hd,df,first_temp_write):
    new_dict = {}
    temp_df = 0
    for key in hd['keys']:
        #print( df['AAA Organization Name'])
        if key in df.keys():
            new_dict[key] = df[key][0]
        else: new_dict[key] = 'SELECT-KEY-WRITE-NULL'
    try:
        temp_df = pd.DataFrame(new_dict, columns = hd['keys'])
    except:
        failed_write.append(org_to_search)


    if first_temp_write == True:
        temp_df.to_csv('org_outputs.csv')
    else:
        try:
            with open('org_outputs.csv', 'a') as f:
                temp_df.to_csv(f, header=False)
        except:
            failed_write.append(org_to_search)




# ------------------------------------- INITIALIZING VARIALBES ----------------------
first_temp_write = True
df_final = pd.DataFrame()
org_dropped_list = []
failed_write = []
#---------------------------------------------------------------------------------


hd = pd.read_csv('part_I_header_list.csv')
org_list = pd.read_csv('org_list.csv') #hardcode read in list of seraching orgs
org_list_with_ein = pd.read_csv('ein-list-2.csv')


org_list_range = range(len(org_list['org_name']))
org_list_with_ein_range = range(len(org_list_with_ein['org_name']))

years = [2018,2017,2016,2015,2014,2013,2012,2011] # years to scrape over
#years = [2018,2017,2016,2015]
#years = [2015]



for year in years:
    print('Running through %i\n'%year)
# --------------------- Initializing variables used on a per-year basis

    found_single = []
    found_multi = []
    found_none = []

    o = 0
    counting_index = 0
# --------------------------------------------------------------------

    pull_data = requests.get('https://s3.amazonaws.com/irs-form-990/index_%i.json'%year) # pulling the index data for the specific year

    # If local:
    #jsondata = open('C:\\Users\\mwatson\\Desktop\\index_2015.json').read()

    jsondata = str(pull_data.text)   #converting the base html to string for json conversion
    data = json.loads(jsondata)     # converting to json

    current_key = []                                 # empty list for quick trick
    for key in data.keys():
        current_key.append(key)  #finds the first and only key (the filing year key) in the top level json
    current_data = data[current_key[0]] #reads into json and creates a list

    for o in org_list_range:
# --------------------- Initializing variables used on a per-organization basis


#-----------------------------------------------------------------------

        org_to_search = org_list['org_name'][o]

        org = get_org(current_data,org_to_search) #searches for organization in the index list.  Uses fuzzy matching, may return more than one
        if len(org) == 0: 
            oo_index = 0
            found_in_oo = False
            #print('trying ein list for %s\n'%org_to_search)
            for oo_index in range(len(org_list_with_ein['org_name'])):
                oo = org_list_with_ein['org_name'][oo_index]
                if org_to_search in oo:
                    found_in_oo = True
                    #print('found %s in ein list'%org_to_search)
                    
                    try:
                        org_to_search_ein = org_list_with_ein['ein'][oo_index]
                        org = get_org(current_data,org_to_search_ein,'ein')
                        df,df_final = org_to_final_df(org,df_final)
                        write_select_key_dict_pairs(hd,df,first_temp_write)
                        #print('Found from none in ein')
                    except: 
                        found_none.append(org_to_search)
            if found_in_oo == False: found_none.append(org_to_search)
                        

        elif len(org) == 1:
            df,df_final = org_to_final_df(org,df_final)

            write_select_key_dict_pairs(hd,df,first_temp_write)
            first_temp_write = False

        elif len(org) > 1:
            found_multi.append(org)
            #print('checking multiple for %s\n'%org_to_search)
            
            oo_index = 0
            for oo_index in range(len(org_list_with_ein['org_name'])):
                oo = org_list_with_ein['org_name'][oo_index]
                if org_to_search in oo:
                    #print('found multi')
                    org_to_search_ein = org_list_with_ein['ein'][oo_index]
                    df,df_final = org_to_final_df(org,df_final)
                    write_select_key_dict_pairs(hd,df,first_temp_write)

            with open('multi-list.csv','a') as f:
                f.write('\nLooking for %s in year %s\n'%(org_to_search,str(year)))
                for orgc in org:
                    to_write = str(orgc) + '\n'
                    f.write(to_write)

        o += 1
    counting_index +=1
