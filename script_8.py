import requests as req
import pandas as pd
import pytd.pandas_td as td
import json
import argparse
import time
import re
import logging
import numpy as np

# Initialize variables containing security key and base url
def main():
    """
    Function:
    Read the readme.md file to check details about the main function of the script.
    
    Parameters:
    args (list of str): Command-line arguments passed to the script.
    """
    # Start the timer
    start_time = time.time()
    logging.basicConfig(level=logging.CRITICAL)
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)
    
    print("START SCRIPT...")
    
    print('---------------------------------------------------------------------------------------------------')
    print('READ INPUT FILE')
    
    # Configuration de l'analyseur d'arguments
    parser = argparse.ArgumentParser(description="Définir le mode d'exécution de l'application")
    parser.add_argument('mode', choices=['development', 'production'], help="Mode d'exécution (development ou production)")
    
    # Analyse des arguments
    args = parser.parse_args()
    
    # Définition de la clé API selon le mode
    if args.mode == 'development':
        api_key = '75/9c1c10996eec2ea8d1401c2a45585522bd7d09d5'
    elif args.mode == 'production':
        api_key = '100/5ac427b7cbef8f9e72ed4ba90e74959fb458db23'
        
    # Exécution de la logique principale avec la clé API définie
    print(f"Exécution en mode {args.mode}")
    
    # Ajoutez ici le reste de votre code qui utilise la clé API
    input_df = pd.read_csv('data/script_8/input.csv')
    input_df.reset_index(drop=True, inplace=True)
    
    print('Input file row filtered: ', len(input_df))
    
    print('---------------------------------------------------------------------------------------------------')
    print('SETUP VARIABLES')
    
    url = f'https://api.eu01.treasuredata.com/v3/job/list?from=0&to=499'
    headers = {
        "Authorization": "TD1 "+ api_key,  # Si l'API nécessite une clé d'API ou un token
    }
    response = req.get(url, headers=headers)
    resp_json = response.json()
    # Verift that the response don't return an error
    
    if response.status_code == 200:
        columns = ['jobID', 'database', 'status','duration','query_size','result_size','priority_order']
        df_jobs = pd.DataFrame(np.nan, index=range(len(resp_json['jobs'])),columns=columns)
        for i in range(len(resp_json['jobs'])):
            if "cdp_audience" in resp_json['jobs'][i]["database"] :
                df_jobs['jobID'][i] = resp_json['jobs'][i]["job_id"]
                df_jobs['database'][i] = resp_json['jobs'][i]["database"]
                df_jobs['status'][i] = resp_json['jobs'][i]["status"]
                df_jobs['duration'][i] = resp_json['jobs'][i]["duration"]
                df_jobs['query_size'][i] = len(resp_json['jobs'][i]["query"])
                df_jobs['result_size'][i] = resp_json['jobs'][i]["result_size"]
        df_jobs = df_jobs[df_jobs['jobID'] != np.nan].reset_index()
        df_jobs = df_jobs[df_jobs['status'] == "success"].reset_index()
        threshold_duration = df_jobs['duration'].quantile(0.75)
        threshold_query_size = df_jobs['query_size'].quantile(0.75)
        df_jobs.loc[(df_jobs['duration'] >= threshold_duration) & (df_jobs['query_size'] >= threshold_query_size), 'priority_order'] = 4
        df_jobs.loc[(df_jobs['duration'] >= threshold_duration) & (df_jobs['query_size'] <= threshold_query_size), 'priority_order'] = 3
        df_jobs.loc[(df_jobs['duration'] <= threshold_duration) & (df_jobs['query_size'] >= threshold_query_size), 'priority_order'] = 2
        df_jobs.loc[(df_jobs['duration'] <= threshold_duration) & (df_jobs['query_size'] <= threshold_query_size), 'priority_order'] = 1
        pd.set_option('display.max_rows', len(df_jobs))
        print(df_jobs.loc[:, 'database'])
        print(len(df_jobs))
    filtered_jobs = df_jobs[df_jobs['priority_order'] == 4]
    print(filtered_jobs['jobID'].to_list())
    list_id = pd.DataFrame(filtered_jobs['jobID'].to_list())
    print(list_id)
    columns = ['ID', 'database', 'query','parent_segment','audienceId','segment_name','last_update']
    dataframe = pd.DataFrame(np.nan, index=range(len(list_id)), columns=columns)
    print(dataframe)
    for index,item in list_id.iterrows():
        if index == 0:
            print(item)
            url = f'https://api.eu01.treasuredata.com/v3/job/show/{item[0]}'
            headers = {
                "Authorization": "TD1 "+ api_key,  # Si l'API nécessite une clé d'API ou un token
            }
            response = req.get(url, headers=headers)
            resp_json = response.json()
            # Verift that the response don't return an error
            if response.status_code == 200: 
                rows = []
                if str(resp_json['job_id']) == str(item[0]):
                    dataframe['ID'][index] = resp_json['job_id']
                    dataframe['database'][index] = resp_json['database']
                    dataframe['query'][index] = resp_json['query']
                
                dataframe['query'][index] = re.sub(r'--.*', '', dataframe['query'][index])
                dataframe['query'][index] = re.sub(r'/\*.*?\*/', '', dataframe['query'][index], flags=re.DOTALL)
                dataframe['query'][index] = re.sub(r'\s+', ' ', dataframe['query'][index]).strip()
                query_df = dataframe['query'][index]
                if 'a."cdp_customer_id"' in query_df:
                    query_df = query_df.replace('a."cdp_customer_id"','a.*',1)
                if ' limit 50' in query_df:
                    query_df = query_df.replace(' limit 50','')
                dataframe['query'][index] = query_df
                query_df = query_df.replace(' ','')
                print(query_df)
                url = "https://api-cdp.eu01.treasuredata.com/audiences" 
                headers = {
                    "Authorization": "TD1 "+ api_key,  # Si l'API nécessite une clé d'API ou un token
                }
                response = req.get(url, headers=headers)
                # Verift that the response don't return an error
                if response.status_code == 200:
                    for parent_segment in response.json():
                        if parent_segment["id"] == dataframe["database"][index][13:19]:
                            name = parent_segment['name']
                            audienceId = parent_segment['id']
                    dataframe['parent_segment'][index] = name
                    dataframe['audienceId'][index] = audienceId
                    url = f"https://api-cdp.eu01.treasuredata.com/audiences/{dataframe['audienceId'][index]}/segments" 
                    headers = {
                        "Authorization": "TD1 "+ api_key,  # Si l'API nécessite une clé d'API ou un token
                    }
                    response = req.get(url, headers=headers)
                    # Verift that the response don't return an error
                    if response.status_code == 200:
                        data_list = []
                        for segment in response.json():
                            data = segment['rule']
                            
                            # Convertir le dictionnaire Python en JSON
                            json_rule = json.dumps(data, indent=4)
                            data_list.append((segment['name'], json_rule,segment['updatedAt']))
                        df = pd.DataFrame(data_list, columns=['name', 'rule','updatedAt'])
                        for i in range(len(df)):
                            url = f"https://api-cdp.eu01.treasuredata.com/audiences/{dataframe['audienceId'][index]}/segments/query"
                            headers = {
                                "Authorization": "TD1 "+ api_key,
                                "Content-Type": "application/json"# Si l'API nécessite une clé d'API ou un token
                            }
                            body = {
                                "format": "sql",
                                "rule": json.loads(df['rule'][i])
                            }
                            response = req.post(url, headers=headers,json=body)
                            # Verift that the response don't return an error
                            if response.status_code == 200 :
                                response = response.json()
                                response['sql'] = ' '.join(response['sql'].split()).replace(' ','')
                                if "selectcount(*)from(" in query_df:
                                    response['sql'] = ("select count(*) from ( " + response['sql'] + " ) cs").replace(' ','')
                                if "TEC-LRAsiaWeekly-Newsletter" in response['sql'] and "TEC-LRBroadband-Newsletter" in response['sql']:
                                    print(response['sql'])
                                if query_df == response['sql']:
                                    #print(dataframe['query'][index])
                                    #print(response['sql'])
                                    dataframe['segment_name'][index] = df['name'][i]
                                    dataframe['last_update'][index] = df['updatedAt'][i]
                            else:
                                print(f"Erreur: {response.status_code}, {response.text}")
                    else:
                        print(f"Erreur: {response.status_code}, {response.text}")
                else:
                    print(f"Erreur: {response.status_code}, {response.text}")
            else:
                print(f"Erreur: {response.status_code}, {response.text}")
            print(dataframe)

if __name__ == "__main__":
    main()