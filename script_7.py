import requests as req
import pandas as pd
import pytd.pandas_td as td
import json
import argparse
import time
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
    input_df = pd.read_csv('data/script_7/input.csv')
    input_df.reset_index(drop=True, inplace=True)
    
    print('Input file row filtered: ', len(input_df))
    
    print('---------------------------------------------------------------------------------------------------')
    print('SETUP VARIABLES')
    
    list_instance = list(input_df['Instance'])
    url = "https://api-cdp.eu01.treasuredata.com/audiences" 
    headers = {
        "Authorization": "TD1 "+ api_key,  # Si l'API nécessite une clé d'API ou un token
    }
    
    print('---------------------------------------------------------------------------------------------------')
    print('TREASURE DATA | GET DIFFERENT SIGNALS FROM TREASURE DATA AUDIENCE STUDIO API' )
    
    response = req.get(url, headers=headers)
    
    # Verift that the response don't return an error
    if response.status_code == 200: 
        rows = []
        for parent_segment in response.json():
            for behavior in parent_segment['behaviors']:
                    for field in behavior['schema']:
                        if parent_segment['name'] in list_instance:
                            row = {
                                'name': parent_segment['name'],
                                'Signal' : behavior['name'],
                                'matrixDatabaseName': behavior['matrixDatabaseName'],
                                'matrixTableName': behavior['matrixTableName']
                            }
                            rows.append(row)
                            
        # Transform the response into a dataframe
        parents_segment = pd.DataFrame.from_dict(rows)
        parents_segment = parents_segment.groupby(['name','Signal','matrixDatabaseName', 'matrixTableName']).apply(list).reset_index()
        parents_segment = parents_segment[['name','Signal','matrixDatabaseName', 'matrixTableName']]
        
        print('---------------------------------------------------------------------------------------------------')
        
        con = td.connect(apikey=api_key, endpoint="https://console-next.eu01.treasuredata.com/")
        td_data = pd.DataFrame()
        
        print('TREASURE DATA | GET DIFFERENT SOURCES FROM TREASURE DATA DATABASE' )
        for index, item in parents_segment.iterrows():
            source_system_list = []
            engine = td.create_engine(f"presto:{item['matrixDatabaseName']}", con=con)
            
            # Create a query based on given parameters, after that send the query to the database and stock the data in the variable data
            query=f"SELECT source_system FROM {item['matrixTableName']} GROUP BY source_system"
            try:
                data = pd.DataFrame(td.read_td_query(query, engine, index_col=None, parse_dates=None, distributed_join=False,  params=None))
                for i in range(len(data)):
                    source_system_list.append(data['source_system'][i])
                item['source_system'] = source_system_list
                df = pd.DataFrame(item)
                df_transposed = df.transpose()
                td_data = pd.concat([td_data, df_transposed], ignore_index=True)
            except:
                pass
        
        # Convert to a list of dictionaries with the desired structure
        print('---------------------------------------------------------------------------------------------------')
        print('SAVE DATA | WRITE DATA TO JSON FILE')
        
        records = td_data.to_dict('records')
        json_records = [
            {
                "parentSegment": record["name"],
                "signal": record["Signal"],
                "sourceSystems": record["source_system"]
            }
            for record in records
        ]
        
        # Convert to JSON
        json_output = json.dumps(json_records, indent=2)
        
        # Save to a JSON file
        with open('data/script_7/output.json', 'w') as f:
            f.write(json_output)
            
        print("END SCRIPT...")
        end_time = time.time()
        elapsed_time = end_time - start_time
    
        # Convert elapsed time to hours, minutes, and seconds
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60
        print(f"Elapsed time: {hours} hours, {minutes} minutes, {seconds:.2f} seconds")
    else:
        print(f"Erreur: {response.status_code}, {response.text}")

if __name__ == "__main__":
    main()

