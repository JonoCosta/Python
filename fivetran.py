import requests
import json
import sys
import pyodbc
import logging
logging.basicConfig(filename='fivetran.log', level=logging.DEBUG)

def metadata():
    url = "https://api.dropboxapi.com/2/files/get_metadata"

    headers = {
            "Authorization": "Bearer OATH_TOKEN_GOES_HERE",
            "Content-Type": "application/json"            
    }

    data = {
            "path": "/API",
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        logging.debug("Response status from dropbox folder query: " + str(response.status_code))
    except requests.exceptions.RequestException as e:
        print(e)
        #logging.debug(print ("The dropbox query failed. Response code: " + e))
        sys.exit(1)
    

    metadata = json.loads(response.content)
    return(metadata)

def modify(dict_Input):
    #removes the nested dictionary shared_info and merges into a single level dictionary which will be used for SQL_Insert function

    temp_Dict = dict_Input['sharing_info']
    dict_Input.pop('sharing_info')
    singleDict = {**dict_Input, **temp_Dict}
    #####convert bool values to strings and return a dictionary containing strings####
    list_encap = [singleDict]
    res = [dict([key, str(value)]  
       for key, value in dicts.items())  
       for dicts in list_encap]
    str_values_dict = res[0]
    logging.debug("Python dictionary generated:\n" + str(str_values_dict))
    return(str_values_dict)

def SQL_Insert(one_level_dict):
    

    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=WINDOWS-IKLC31K\SQLEXPRESS;'
                        'Database=stuff;'
                        'UID=sa;'
                        'PWD=password12345;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor()
    except (pyodbc.error, pyodbc.OperationalError) as ex:
        logging.error(str(ex))
        sys.exit(1)

    values = []
    for i in one_level_dict.values():
        values.append(i)

    try:
        cursor.execute("""insert into stuff.dbo.Table_1 VALUES (?,?,?,?,?,?,?,?,?);""", values)
        cursor.commit()
        ###validate file id passed is now in the db###
        db_file_id = []
        db_file_id.append(str(one_level_dict.get('id')))
        cursor.execute("""select id from stuff.dbo.Table_1 where id = ?;""", db_file_id)
        queried_fileid = cursor.fetchone()
        logging.debug("query successful. file ID added: " + str(queried_fileid))
        db_file_id = cursor.execute("""SELECT id from stuff.dbo.Table_1 """)
        
    except Exception as e:
        logging.debug("query failed with error," + e)
    
    conn.close()

########    
folder_metadata = metadata()
cleaned_data = modify(folder_metadata)
SQL_Insert(cleaned_data)