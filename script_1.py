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

    # Eloqua Workspace currently in Postman
    POSTMAN_API_URL_PATH = "/workspaces/a337ef19-1b78-4425-8307-a894259641a8"

    print('---------------------------------------------------------------------------------------------------')
    print('GET POSTMAN ELOQUA WORKSPACE')
    print('POSTMAN | HTTP Request: ' + POSTMAN_API_URL_PATH)

    response = utils.get_list_of_items(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, POSTMAN_API_URL_PATH)
    envs.extend(response.json().get('workspace', {}).get('environments', []))

    print('---------------------------------------------------------------------------------------------------')
    print('GET EACH POSTMAN ENVIRONMENT IN ELOQUA WORKSPACE')
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
            
            # Add the POD to the environment
            if( value['key'] == 'elq-Env-Pod' ):
                item['pod'] = value['value']

            # Add the Base64 Encoding string to the environment
            if( value['key'] == 'elq-Env-BasicAuthEncoding' ):
                item['api_key'] = value['value']


        envs_details.append(item)

    print('---------------------------------------------------------------------------------------------------')
    print('FILTER ONLY RELEVANT POSTMAN ENVIRONMENTS')
    final_envs = [env_detail for env_detail in envs_details if 'env' in env_detail and 'tool' in env_detail and env_detail['env'] == 'prd' and env_detail['tool'] == 'elq']
    print('Eloqua environments filtered: ', len(final_envs))

    print('---------------------------------------------------------------------------------------------------')
    print('GET CAMPAIGN_ANALYSIS | EMAIL_ACTIVITIES FROM ELOQUA REPORTING API ')
        
    for final_env in final_envs:
        print('------')
        print('ELOQUA | ENV NAME: ' + final_env['name'] + ' | START...')
        
        # Declare API Variables
        POD = final_env['pod']
        API_KEY = final_env['api_key']
        
        ELOQUA_AUTHORIZATION = {
            'key': 'Authorization',
            'value': f'Basic {API_KEY}'
        }

        ELOQUA_API_BASE_URL = f'https://secure.{POD}.eloqua.com'
        API_URL_PATH = "/api/odata/1.0/campaignAnalysis/emailActivities"

        # Initialize an empty list to store all segments
        items = []
        
        # Initailise variable for the loop
        page_number = 0
        top = 5000
        skip = 0  # Adjust the page size as needed
        
        
        while True:
            API_URL_PARAMS = f'?$orderby=eloquaCampaignId desc&$count=true&$top={top}&$skip={skip}'
            API_URL_FULL_PATH = API_URL_PATH + API_URL_PARAMS
            print("ELOQUA | HTTP Request: " + API_URL_FULL_PATH)

            # Call reusable function which will get full list ofContact fields in Eloqua
            response = utils.get_list_of_items(ELOQUA_AUTHORIZATION, ELOQUA_API_BASE_URL, API_URL_FULL_PATH)
            items.extend(response.json().get('value', []))

            count = response.json().get('@odata.count', 0)
            
            number_loops = math.ceil(count / top)
            if page_number < number_loops :
                page_number = page_number + 1
                skip = skip + top

            else:
                print("ELOQUA | ITEMS RETURNED BY API: ", count)
                print("ELOQUA | ITEMS RETURNED IN LIST: ", len(items))

                # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
                utils.write_dataframe_to_csv(pd.DataFrame.from_dict(items), 'data/campaignEmailActivities.csv')
                break
        print('ELOQUA | ENV NAME: ' + final_env['name'] + ' | END...')
    
    print("END SCRIPT...")

if __name__ == "__main__":
    # sys.argv contains the command-line arguments passed to the script, 
    # with the script name at index 0 and the rest of the arguments following.
    main(sys.argv[1:])