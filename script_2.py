import sys
import math
import os
import pandas as pd
import requests as rt



    # Create a reusable function to get a list of items:
def get_an_item(authorization, base_url, path, query_string=None):

    # Setup REST API http requests header
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }  
    
    if query_string is None:
        try:
            response = rt.get(base_url + path, headers=headers)
            response.raise_for_status()

        except rt.exceptions.HTTPError as error:
            print(f'There is an error: {error} ')
            return None
    else:
        try:
            response = rt.get(base_url + path, headers=headers, params=query_string)
            response.raise_for_status()

        except rt.exceptions.HTTPError as error:
            print(f'There is an error: {error} ')
            return None
    
    return response

# Create a reusable function to get a list of items:
def get_list_of_items(authorization, base_url, path, query_string=None):

    # Setup REST API http requests header
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }  
    
    if query_string is None:
        try:
            response = rt.get(base_url + path, headers=headers)
            response.raise_for_status()

        except rt.exceptions.HTTPError as error:
            print(f'There is an error: {error} ')
            return None
    else:
        try:
            response = rt.get(base_url + path, headers=headers, params=query_string)
            response.raise_for_status()

        except rt.exceptions.HTTPError as error:
            print(f'There is an error: {error} ')
            return None
    
    return response

# Create a reusable function to get a list of items:
def post_an_item(authorization, base_url, path, payload):

    # Setup REST API http requests header
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authorization
    }  
    
    try:
        response = rt.post(base_url + path, headers=headers, data=payload)
        response.raise_for_status()

    except rt.exceptions.HTTPError as error:
        #print(f'There is an error: {error}')
        return None
    
    return response

def write_dataframe_to_csv(df, file_path):
    if not os.path.exists('data'):
        os.makedirs('data')
    
    df.to_csv(file_path, index=False)


# Main Function
def main(args):
    """
    Function:
    Check the readme.md file to check details about the main function of the script.

    Parameters:
    args (list of str): Command-line arguments passed to the script.
    """

    print("Arguments passed to the script:", args)
    
    # Declare API Variables
    POD = 'p04'
    API_KEY = 'SW5mb3JtYTExXG1hcnkud2FsbGFjZTpSdXN0eTEyMw=='
    AUTHORIZATION = f'Basic {API_KEY}'
    ELOQUA_API_BASE_URL = f'https://secure.{POD}.eloqua.com'
    API_URL_PATH = "/api/odata/1.0/ActivityDetails/emailGroupSubscriptionStatus"

    # Initialize an empty list to store all segments
    items = []
    
    # Initailise variable for the loop
    page_number = 0
    top = 10000
    skip = 0  # Adjust the page size as needed

    while True:
        API_URL_PARAMS = f'?$filter=emailGroupID eq 100&$orderby=contactID desc&$count=true&$top={top}&$skip={skip}'
        API_URL_FULL_PATH = API_URL_PATH + API_URL_PARAMS
        print("HTTP Request: " + API_URL_FULL_PATH)

        # Call reusable function which will get full list ofContact fields in Eloqua
        response = get_list_of_items(AUTHORIZATION, ELOQUA_API_BASE_URL, API_URL_FULL_PATH)
        items.extend(response.json().get('value', []))

        count = response.json().get('@odata.count', 0)
        
        number_loops = math.ceil(count / top)
        if page_number < number_loops :
            page_number = page_number + 1
            skip = skip + top

        else:
            print("Number of items returned by API: ", count)
            print("Number of items in the list: ", len(items))

            # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
            write_dataframe_to_csv(pd.DataFrame.from_dict(items), 'data/activityDetailsEmailGroupSubscriptionStatus.csv')
            break
    
    print("END...")

if __name__ == "__main__":
    # sys.argv contains the command-line arguments passed to the script, 
    # with the script name at index 0 and the rest of the arguments following.
    main(sys.argv[1:])