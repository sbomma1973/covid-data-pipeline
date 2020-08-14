## Scrpit to buil Data Pipeline into Elastic
# One would have already setup the repository
# Repository folder, credentials and ES connection is hardcoded -- Will be refactored to be configurable.
# Uses basic Rest authentication and rest interface to interact
# Index pattern is already created in Elastic (covid*)
# Daily Index is created for every csv file using the csv ingest processor
# This scrpit can be scheduled with tools like cron

# Imports
import os
from os import listdir
import requests;
from datetime import datetime
from requests.auth import HTTPBasicAuth
import json
import logging
import csv




#date time setup for the script

logging.info('Setting Datetime:')
now = datetime.now();
dt_string = now.strftime("%m%d%Y %H:%M:%S")
logging.info('Executing Ingest Script:', dt_string)

#functions

#function to execute search
def search_ (uri, term, username,  password):
    #""Simple Elasticsearch query""
    #json_data = updateJson(sys.argv[1])
    headers = {'Content-Type':'application/json','Connection' : 'close'}
    stringterm = term
    print ('********term*********', stringterm)
    #search = {'query': {'match_all': {}}}
    search = {"query": {"term": {"Province_State": "Alabama"}}}

    query = json.dumps(search)
    response = requests.get(uri, data = query, auth=HTTPBasicAuth(username, password),  headers=headers)
    #response = requests.get(uri, data=query)
    print ('response:', response)

    results = json.loads(response.text)
    print ('***********request ended *********')
    return results


#function to update the repo
def repo_update_fn():
    print ('***************** Updating Repository:', dt_string)
    # os.system(
        #'cd /Users/satishbomma/Coviddata/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports; git pull --verbose')

    os.system('pwd')

# Function to setup ingest pipeline with csv and convert processors
def create_ingest_pipeline(url, username, password):
    # ""Simple Elasticsearch query""
    # json_data = updateJson(sys.argv[1])
    headers = {'Content-Type': 'application/json', 'Connection' : 'close'}
    pipeline_str = {"description": "Ingest pipeline created by file structure finder",
                    "processors": [{"csv":{"field": "message","target_fields" :
                        [ "Province_State", "Country_Region", "Last_Update", "Lat", "Long_",  "Confirmed", "Deaths",
                          "Recovered", "Active", "FIPS", "Incident_Rate","People_Tested", "People_Hospitalized",
                          "Mortality_Rate","UID", "ISO3", "Testing_Rate","Hospitalization_Rate"
                        ], "ignore_missing": False}}
                        ,{ "date": { "field": "Last_Update", "timezone": "{{ event.timezone }}"
                            , "formats": [ "yyyy-MM-dd HH:mm:ss"  ] } }
                        , { "convert": { "field": "Active", "type": "double","ignore_missing": True}}
                        , { "convert": { "field": "Confirmed","type": "long", "ignore_missing": True }}
                        , { "convert": { "field": "Deaths", "type": "long", "ignore_missing": True}}
                        , { "convert": { "field": "FIPS","type": "long", "ignore_missing": True }}
                        , { "convert": { "field": "Hospitalization_Rate", "type": "double", "ignore_missing": True}}
                        , { "convert": { "field": "Incident_Rate","type": "double","ignore_missing": True}}
                        , { "convert": { "field": "Lat","type": "double","ignore_missing": True}}
                        , { "convert": { "field": "Long_", "type": "double","ignore_missing": True }}
                        , { "convert": { "field": "Mortality_Rate", "type": "double",  "ignore_missing": True}}
                        , { "convert": { "field": "People_Hospitalized", "type": "long", "ignore_missing": True}}
                        , { "convert": { "field": "People_Tested",  "type": "long","ignore_missing": True}}
                        , { "convert": { "field": "Recovered", "type": "long", "ignore_missing": True }}
                        , { "convert": { "field": "Testing_Rate",  "type": "double", "ignore_missing": True}}
                        , { "convert": { "field": "UID", "type": "long", "ignore_missing": True }}
                        , {"remove": {"field": "message" }}]}
    pipeline = json.dumps(pipeline_str)
    pipe_response = requests.put(url, data = pipeline, auth=HTTPBasicAuth(username, password),  headers=headers)
    print ("response", pipe_response)

    results = json.loads(pipe_response.text)
    return results

def delete_index (url,  username, password, index_name):
    print ("deleting index -- ", index_name)
    url  = url + index_name
    print (url)
    headers = {'Content-Type': 'application/json', 'Connection': 'close'}
    mapping_resp = requests.delete(url, auth=HTTPBasicAuth(username, password), headers=headers)
    print ("delete reps--", mapping_resp)
    return mapping_resp



def create_index_mapping(url, username, password, index_name):
    print ("INFO: --- Create index mapping function:")
    url = url + index_name
    print (url)
    headers = {'Content-Type': 'application/json', 'Connection': 'close'}
    mappinig_str = {
        "mappings":{
            "_meta" :{
                "created by" : "script"
            }
        ,
        "properties" : {
        "@timestamp" : {
            "type" : "date"
        },
        "Active" : {
            "type" : "double"
        },
        "Confirmed" : {
            "type" : "long"
        },
        "Country_Region" : {
            "type" : "keyword"
        },
        "Deaths" : {
            "type" : "long"
        },
        "FIPS" : {
            "type" : "long"
        },
        "Hospitalization_Rate" : {
            "type" : "double"
        },
        "ISO3" : {
            "type" : "keyword"
        },
        "Incident_Rate" : {
            "type" : "double"
        },
        "Last_Update" : {
            "type" : "date",
            "format" : "yyyy-MM-dd HH:mm:ss"
        },
        "Lat" : {
            "type" : "double"
        },
        "Long_" : {
            "type" : "double"
        },
        "Mortality_Rate" : {
            "type" : "double"
        },
        "People_Hospitalized" : {
            "type" : "long"
        },
        "People_Tested" : {
            "type" : "long"
        },
        "Province_State" : {
            "type" : "keyword"
        },
        "Recovered" : {
            "type" : "long"
        },
        "Testing_Rate" : {
            "type" : "double"
        },
        "UID" : {
            "type" : "long"
        }}}}
    mapping = json.dumps(mappinig_str)
    mapping_resp = requests.put(url,data=mapping, auth=HTTPBasicAuth(username, password), headers=headers)

    print ("index mappinig function response", mapping_resp.text)

    return mapping_resp.text



def get_index_mapping (url, username, password, index_name):
    print ("INFO: find mapping fun")
    url=url+index_name+"/_mapping"
   # print (url)
    headers = {'Content-Type': 'application/json', 'Connection': 'close'}
    #mapping = json.dumps(mappinig_str)
    mapping_resp = requests.get(url,  auth=HTTPBasicAuth(username, password),  headers=headers)
    print ("response", mapping_resp.text)
    if "404" in mapping_resp.text:
        return "True"
    else:
        return "False"

# Main function to get executed to process rest of the files
if __name__ == '__main__':
    logging.info(dt_string + 'test')
    repo_update_fn()
    path="/Users/satishbomma/Coviddata/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/"
    url = "https://33e09c9e1d204546af0289c6ad97fcd6.us-central1.gcp.cloud.es.io:9243/covid*/_search"
    urlpipeline = "https://33e09c9e1d204546af0289c6ad97fcd6.us-central1.gcp.cloud.es.io:9243/_ingest/pipeline/COVID_PIPELINE_US"
    urlmapping="https://33e09c9e1d204546af0289c6ad97fcd6.us-central1.gcp.cloud.es.io:9243/"


    username = "elastic"
    password = "VoUxGCZVxUaynYjjcjK46Q9C"

    print ('url:', url)
    print (username)
    print (password)

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/

    # x = requests.get(url,auth=HTTPBasicAuth(username,password))
    term = 'satish'
    #response = search_(url, term, username, password)
    #print(response)

    response1 = create_ingest_pipeline(urlpipeline, username, password)
   # print (response1)

    #response2 = create_mappinig()

    arr = os.listdir ('/Users/satishbomma/Coviddata/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/')
    #print (arr)
    for file in arr:
        file_name = file.rsplit('.', 1)[0]
        file1 = path + file
        #print (file)
        index_name = "indexcovid-"+file_name
        #print ("indexname:", index_name)
        result = get_index_mapping(urlmapping,username, password, index_name)
        print (result)
        if result == "True":
            resp=create_index_mapping (urlmapping, username, password, index_name)
            #print resp
            file_path = path+file
            print (file_path)
            url_insert = urlmapping+index_name+"?pipeline=COVID_PIPELINE_US"
            with open(file1) as f_obj:
                  #reader = csv.DictReader(f_obj)
                  headers = {'Content-Type': 'application/json', 'Connection':'close'}
                  result1 = requests.put(url_insert ,data=f_obj, auth=HTTPBasicAuth(username, password), headers=headers )
                  print (url_insert)
                  print (file)
                  #print (csvFile.writelines())
                  print (result1.text)

                  f_obj.close()


        else:
            resp=delete_index(urlmapping, username, password, index_name)
            print ("Index exists", resp)


#print(x.text)



