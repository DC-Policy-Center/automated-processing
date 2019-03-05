




def list_to_string(lst):
    list_string = ''
    for item in lst:
        list_string = list_string +',\n'+ str(item) + '\n'
    return(list_string)
def write_html(item,lst):
    html_head = "<head></head>\n"

    html_header = "<h1>This is my header</h1>\n"
    html_counter = "<p>This is my counter should change in place: {}</p>\n".format(item)
    html_list =  " <p>This is a list that should append\n{}\n</p>".format(lst)

    html_body ="<body>{}{}{}</body>".format(html_header,html_counter,html_list)

    full_html = "{}{}".format(html_head,html_body)

    with open('output.html','w') as f:
        f.write(full_html)
    f.close()

i = 0
j = []


for i in range(10000):
    i_str = str(i)
    j.append(i_str)
    list_string = list_to_string(j)
    write_html(i_str,list_string)
    i+=1
