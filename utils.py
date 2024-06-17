import os
import requests as rt

# Create a reusable function to get a list of items:
def get_an_item(authorization, base_url, path, query_string=None):

    # Setup REST API http requests header
    headers = {
        'Content-Type': 'application/json',
        authorization['key']: authorization['value']
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
        authorization['key']: authorization['value']
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
        authorization['key']: authorization['value']
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

# Custom function to calculate the range (max - min)
def calc_range(x):
    return (x > 5).sum()
   