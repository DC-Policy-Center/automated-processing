import json
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

def xml_tag_selector(xml_soup,selector):
    found = xml_soup(selector)
    r = 'NULL'
    try:
        for node in found[0]:
            r = ''.join(node)
    except:
        r = 'NULL'
    if r == '\n': r = 'EMPTY-NULL'
    return(r)

def get_org(data,search_name):
    i = 0
    orgs = []
    for i in range(len(data)):
        org_name = data[i]['OrganizationName'].lower()
        url = data[i]['URL']
        #search_name = 'higher achievement'
        search_name = search_name.lower()
        if search_name in org_name:
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


df_empty = True

org_list = pd.read_csv('C:\\users\\mwatson\\desktop\\org_list.csv') #hardcode read in list of seraching orgs
drop_list = []

years = [2018,2017,2016,2015,2014,2013,2012,2011] # years to scrape over
#years = [2015]


for year in years:
    pull_data = requests.get('https://s3.amazonaws.com/irs-form-990/index_%i.json'%year) # pulling the index data for the specific year
    print('Running through %i\n'%year)
    # If local:
    #jsondata = open('C:\\Users\\mwatson\\Desktop\\index_2015.json').read()

    jsondata = str(pull_data.text)   #converting the base html to string for json conversion
    data = json.loads(jsondata)     # converting to json

    current_key = []                                 # empty list for quick trick
    for key in data.keys():
        current_key.append(key)  #finds the first and only key (the filing year key) in the top level json
    current_data = data[current_key[0]] #reads into json and creates a list
    o = 0
    found_single = []
    found_multi = []
    found_none = []


    counting_index = 0
    for o in range(len(org_list['org_name'])):
        org_to_search = org_list['org_name'][o]
        org = get_org(current_data,org_to_search) #searches for organization in the index list.  Uses fuzzy matching, may return more than one
        if len(org) == 1:
            url = org[0][1]                        # saves url from org return
            found_single.append(org)
            nn = nn_request(url)            # retrieves 990 info from aws/irs
        # breaking out dict return from the 990
            soup = nn['soup']
            headers = nn['headers']
            values = []
            for header in headers:
                values.append(xml_tag_selector(soup,header))

            df = pd.DataFrame(columns = headers)
            df.loc[0] = values
            if df_empty == True:
                df_final = df
                df_empty = False
            else:
                df_final = df_header_combine(df_final,df)
            df.to_csv('C:\\Users\\mwatson\\Desktop\\outputs\\'+org[0][0]+'.csv')
            drop_list.append(org_list.index[o])
            org_list.drop(org_list.index[o])

        elif len(org) > 1:
            found_multi.append(org)
        else:
            found_none.append(org)
        o += 1
    counting_index +=1

print('writting output now')
df_final.to_csv('output.csv')


with open('orgs_found.csv','w') as f:
    for item in drop_list:
        f.write(str(item)+'\n')



'''
    nn = nn_request(url)            # retrieves 990 info from aws/irs

    rows = []

    for i in range(len(items)):
        row = items[i] + ',' + xml_tag_selector(soup,items[i])
        rows.append(row)


    with open('output.csv','w') as f:
        for r in rows:
            f.write(r+'\n')

    # breaking out dict return from the 990
    soup = nn['soup']
    headers = nn['headers']


    df = pd.DataFrame(columns = headers)
    if counting_index == 0:
        df_new = df
    else: df_new = df.append(df)

'''
