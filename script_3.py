from dotenv import load_dotenv
import sys
import math
import os
import pandas as pd
import requests as rt

import utils

# Load environment variables from a .env file
load_dotenv()

# Main Function
def main(args):
    """
    Function:
    Read the readme.md file to check details about the main function of the script.

    Parameters:
    args (list of str): Command-line arguments passed to the script.
    """

    print("Arguments passed to the script:", args)
    print("START SCRIPT...")

    envs = []
    envs_details = []
    
    POSTMAN_AUTHORIZATION = {
    'key': 'X-Api-Key',
    'value': os.getenv('POSTMAN_API_KEY')
    }
    
    POSTMAN_API_BASE_URL = f'https://api.getpostman.com'

    # Treasure Data Workspace currently in Postman
    POSTMAN_API_URL_PATH = "/workspaces/fa5acd7d-11a4-4aad-b02f-b322cdcdc336"

    print('---------------------------------------------------------------------------------------------------')
    print('GET POSTMAN TREASURE DATA WORKSPACE')
    print('POSTMAN | HTTP Request: ' + POSTMAN_API_URL_PATH)

    response = utils.get_list_of_items(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, POSTMAN_API_URL_PATH)
    envs.extend(response.json().get('workspace', {}).get('environments', []))

    print('---------------------------------------------------------------------------------------------------')
    print('GET EACH POSTMAN ENVIRONMENT IN TREASURE DATA WORKSPACE')
    for env in envs:
        
        POSTMAN_API_URL_FULL_PATH = '/environments/' + env['uid']
        print('POSTMAN | HTTP Request: ' + POSTMAN_API_URL_FULL_PATH + ' | ENV NAME: ' + env['name'])

        response = utils.get_an_item(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, POSTMAN_API_URL_FULL_PATH)
        resp_body = response.json().get('environment', {})

        item = {
           'id': resp_body['id'],
           'name': resp_body['name'],
           'createdAt': resp_body['createdAt'],
           'updatedAt': resp_body['updatedAt'],
        }

        for value in resp_body['values']:
            
            # Add the environment Type to the environment
            if( value['key'] == 'env' ):
                item['env'] = value['value']
            
            # Add the tool to the environment
            if( value['key'] == 'tool' ):
                item['tool'] = value['value']

            # Add the Base64 Encoding string to the environment
            if( value['key'] == 'td-Env-BasicAuthEncoding' ):
                item['api_key'] = value['value']


        envs_details.append(item)

    print('---------------------------------------------------------------------------------------------------')
    print('FILTER ONLY RELEVANT POSTMAN ENVIRONMENTS')
    final_envs = [env_detail for env_detail in envs_details if 'env' in env_detail and 'tool' in env_detail and env_detail['env'] == 'prd' and env_detail['tool'] == 'td']
    print('Treasure Data environments filtered: ', len(final_envs))

    print('---------------------------------------------------------------------------------------------------')
    print('GET PARENT SEGMENTS FROM TREASURE DATA AUDIENCE STUDIO API ')
        
    for final_env in final_envs:
        print('------')
        print('TREASURE DATA | ENV NAME: ' + final_env['name'] + ' | START...')
        
        # Declare API Variables
        API_KEY = final_env['api_key']
        
        TD_AUTHORIZATION = {
            'key': 'Authorization',
            'value': f'TD1 {API_KEY}'
        }

        TD_API_BASE_URL = f'https://api-cdp.eu01.treasuredata.com'
        

        # Initialize an empty list to store all parent segments
        parent_segments = []        
        
        
        
        API_URL_PATH = "/audiences"
        print("TREASURE DATA | HTTP Request: " + API_URL_PATH)

        # Call reusable function which will get full list parent segments in Eloqua
        response = utils.get_list_of_items(TD_AUTHORIZATION, TD_API_BASE_URL, API_URL_PATH)
        
        
        for parent_segment in response.json():
            row = {
                'name': parent_segment['name'],
                'id': parent_segment['id'],
            }
            parent_segments.append(row)

        # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
        utils.write_dataframe_to_csv(pd.DataFrame.from_dict(parent_segments), 'data/script_3/td_parent_segments.csv')

        print('TREASURE DATA | ENV NAME: ' + final_env['name'] + ' | END...')

        print('GET SEGMENTS IN TREASURE DATA PARENT SEGMENTS')
        segments = []
        
        for parent_segment in parent_segments:
            print('/audiences/'+ parent_segment['id']+ '/segments')
            API_URL_PATH ="/audiences/"+ parent_segment['id'] + "/segments"
            
            response = utils.get_list_of_items(TD_AUTHORIZATION, TD_API_BASE_URL, API_URL_PATH)

            # Loop through each segment item within a parent segment, create a row and add it to a list
            for item in response.json():
                row = {
                    'audienceId': item['audienceId'],
                    'id': item['id'],
                    'name': item['name'],
                }
                segments.append(row)
            
        # Convert a JSON input to a pandas DataFrame
        df = pd.DataFrame.from_dict(segments)
        # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
        utils.write_dataframe_to_csv(df, 'data/script_3/td_segments.csv')

        # Filter the DataFrame based on two columns
        filtered_df = df[(df['audienceId'] == '299246') & (df['id'] == '301059')]

        # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
        utils.write_dataframe_to_csv(filtered_df, 'data/script_3/td_segments.csv')

        
    print("END SCRIPT...")

if __name__ == "__main__":
    # sys.argv contains the command-line arguments passed to the script, 
    # with the script name at index 0 and the rest of the arguments following.
    main(sys.argv[1:])