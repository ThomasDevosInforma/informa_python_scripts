from dotenv import load_dotenv
import sys
import math
import os
import json
import pandas as pd
import requests as rt
import pytd.pandas_td as td

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
   
    POSTMAN_API_BASE_URL = f'https://api.getpostman.com'
    
    POSTMAN_AUTHORIZATION = {
    'key': 'X-Api-Key',
    'value': os.getenv('POSTMAN_API_KEY')
    }

    print('---------------------------------------------------------------------------------------------------')
    print('GET POSTMAN ELOQUA WORKSPACE')
    elq_envs = []
    elq_envs_details = []
    
    # Eloqua Workspace currently in Postman
    ELQ_POSTMAN_API_URL_PATH = "/workspaces/a337ef19-1b78-4425-8307-a894259641a8"

    print('POSTMAN | HTTP Request: ' + ELQ_POSTMAN_API_URL_PATH)

    response = utils.get_list_of_items(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, ELQ_POSTMAN_API_URL_PATH)
    elq_envs.extend(response.json().get('workspace', {}).get('environments', []))

    print('---------------------------------------------------------------------------------------------------')
    print('GET EACH POSTMAN ENVIRONMENT IN ELOQUA WORKSPACE')
    for elq_env in elq_envs:
        
        POSTMAN_API_URL_FULL_PATH = '/environments/' + elq_env['uid']
        print('POSTMAN | HTTP Request: ' + POSTMAN_API_URL_FULL_PATH + ' | ENV NAME: ' + elq_env['name'])

        response = utils.get_an_item(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, POSTMAN_API_URL_FULL_PATH)
        resp_body = response.json().get('environment', {})

        item = {
           'id': resp_body['id'],
           'name': resp_body['name'],
           'createdAt': resp_body['createdAt'],
           'updatedAt': resp_body['updatedAt'],
        }

        for value in resp_body['values']:
            
            # Add the active attribute to the environment
            if( value['key'] == 'active' ):
                item['active'] = value['value']
            
            # Add the environment Type to the environment
            if( value['key'] == 'env' ):
                item['env'] = value['value']
            
            # Add the POD to the environment
            if( value['key'] == 'elq-Env-Pod' ):
                item['pod'] = value['value']

            # Add the Base64 Encoding string to the environment
            if( value['key'] == 'elq-Env-BasicAuthEncoding' ):
                item['api_key'] = value['value']


        elq_envs_details.append(item)

    print('---------------------------------------------------------------------------------------------------')
    print('FILTER ONLY RELEVANT ELOQUA POSTMAN ENVIRONMENTS')
    elq_final_envs = [elq_env_detail for elq_env_detail in elq_envs_details if 'env' in elq_env_detail and elq_env_detail['env'] == 'prd' and 'active' in elq_env_detail and elq_env_detail['active'] == 'true' ]
    print('Eloqua environments filtered: ', len(elq_final_envs))
    
    print('---------------------------------------------------------------------------------------------------')        
    for elq_final_env in elq_final_envs:
        print('ELOQUA | ENV NAME: ' + elq_final_env['name'] + ' | START...')
        
        # Declare API Variables
        POD = elq_final_env['pod']
        API_KEY = elq_final_env['api_key']
        
        ELOQUA_AUTHORIZATION = {
            'key': 'Authorization',
            'value': f'Basic {API_KEY}'
        }

        ELOQUA_API_BASE_URL = f'https://secure.{POD}.eloqua.com'

        # GET Email Activity Data
        print('------')
        print('GET CAMPAIGN_ANALYSIS | EMAIL_ACTIVITIES FROM ELOQUA REPORTING API ')

        API_URL_PATH_ACTIVITY = "/api/odata/campaignAnalysis/1/emailActivities"

        # Initialize an empty list to store all segments
        activity_items = []
        
        # Initailise variable for the loop
        page_number = 0
        top = 5000
        skip = 0  # Adjust the page size as needed
        
        # GET Email Activity Data
        while True:
            API_URL_PARAMS_ACTIVITY = f'?$orderby=eloquaCampaignId desc&$count=true&$expand=emailAsset,campaign&$top={top}&$skip={skip}&$select=dateHour,eloquaCampaignId,emailId,totalSends,totalOpens,totalClickthroughs'
            API_URL_FULL_PATH_ACTIVITY = API_URL_PATH_ACTIVITY + API_URL_PARAMS_ACTIVITY
            print("ELOQUA | HTTP Request: " + API_URL_FULL_PATH_ACTIVITY)

            # Call reusable function which will get full list ofContact fields in Eloqua
            response = utils.get_list_of_items(ELOQUA_AUTHORIZATION, ELOQUA_API_BASE_URL, API_URL_FULL_PATH_ACTIVITY)
            activity_items.extend(response.json().get('value', []))

            count = response.json().get('@odata.count', 0)
            
            number_loops = math.ceil(count / top)
            if page_number < number_loops :
                page_number = page_number + 1
                skip = skip + top

            else:
                print("ELOQUA | ITEMS RETURNED BY API: ", count)
                print("ELOQUA | ITEMS RETURNED IN LIST: ", len(activity_items))
                break        
    
        print('------')
        print('TRANSFORM DATA | CLEAN & JOIN TABLES USING PANDAS')


        # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
        activity_df = pd.json_normalize(activity_items,max_level=1)
        activity_df = activity_df.rename(columns={'emailAsset.emailGroup': 'emailGroup', 'emailAsset.emailGroupID': 'emailGroupID', 'campaign.campaignName': 'CampaignName', 'emailAsset.emailName': 'emailName'})

        activity_df['instance'] = elq_final_env['name']

        # convert the date string into UTC datetime and calculate relative days.
        today = pd.Timestamp.now(tz='UTC').normalize()
        activity_df['dateTime'] = pd.to_datetime(activity_df['dateHour'])
        activity_df['dateUtc'] = activity_df['dateTime'].apply(lambda x: x.tz_convert('UTC')).dt.normalize()
        activity_df['relative_days'] = (activity_df['dateUtc'] - today).dt.days

        # Filter out data that is more than 2 years using daily rolling
        filtered_activity_df = activity_df[activity_df['relative_days'] >= -730]
        filtered_activity_df = filtered_activity_df[['instance', 'emailGroup', 'CampaignName', 'emailName', 'totalSends', 'totalOpens', 'totalClickthroughs']]
        
        activity_group_df = filtered_activity_df.groupby(['instance', 'emailGroup', 'CampaignName', 'emailName']).sum().reset_index()

        print('------')
        print('SAVE DATA | WRITE DATA TO CSV FILE')
        utils.write_dataframe_to_csv(activity_df, 'data/script_1/campaignEmailActivities.csv')
        utils.write_dataframe_to_csv(activity_group_df, 'data/script_1/campaignEmailActivities_elq.csv')

        print('ELOQUA | ENV NAME: ' + elq_final_env['name'] + ' | END...')

    
    print('---------------------------------------------------------------------------------------------------')
    print('GET POSTMAN TREASURE DATA WORKSPACE')

    td_envs = []
    td_envs_details = []

    # Treasure Data Workspace currently in Postman
    TD_POSTMAN_API_URL_PATH = "/workspaces/fa5acd7d-11a4-4aad-b02f-b322cdcdc336"

    print('POSTMAN | HTTP Request: ' + TD_POSTMAN_API_URL_PATH)

    response = utils.get_list_of_items(POSTMAN_AUTHORIZATION, POSTMAN_API_BASE_URL, TD_POSTMAN_API_URL_PATH)
    td_envs.extend(response.json().get('workspace', {}).get('environments', []))

    print('---------------------------------------------------------------------------------------------------')
    print('GET EACH POSTMAN ENVIRONMENT IN TREASURE DATA WORKSPACE')
    for td_env in td_envs:
        
        POSTMAN_API_URL_FULL_PATH = '/environments/' + td_env['uid']
        print('POSTMAN | HTTP Request: ' + POSTMAN_API_URL_FULL_PATH + ' | ENV NAME: ' + td_env['name'])

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

            # Add the Base64 Encoding string to the environment
            if( value['key'] == 'td-Env-BasicAuthEncoding' ):
                item['api_key'] = value['value']

        td_envs_details.append(item)
    
    print('---------------------------------------------------------------------------------------------------')
    print('FILTER ONLY RELEVANT TREASURE DATA POSTMAN ENVIRONMENTS')
    td_final_envs = [td_env_detail for td_env_detail in td_envs_details if 'env' in td_env_detail and td_env_detail['env'] == 'prd']
    print('Treasure Data environments filtered: ', len(td_final_envs))
    
    print('---------------------------------------------------------------------------------------------------')        
    for td_final_env in td_final_envs:
        print('TREASURE DATA | ENV NAME: ' + td_final_env['name'] + ' | START...')
        print('TREASURE DATA | GET PARENT SEGMENTS FROM TREASURE DATA AUDIENCE STUDIO API' )

        # Declare API Variables
        API_KEY = td_final_env['api_key']
        
        TD_AUTHORIZATION = {
            'key': 'Authorization',
            'value': f'TD1 {API_KEY}'
        }

        TD_API_BASE_URL = f'https://api-cdp.eu01.treasuredata.com'
        
        # Initialize an empty list to store all parent segments
        rows = []     
        
        API_URL_PATH = "/audiences"
        print("TREASURE DATA | HTTP Request: " + API_URL_PATH)

        # Call reusable function which will get full list parent segments in Eloqua
        response = utils.get_list_of_items(TD_AUTHORIZATION, TD_API_BASE_URL, API_URL_PATH)
        
        for parent_segment in response.json():
            for behavior in parent_segment['behaviors']:
                for field in behavior['schema']:
                    row = {
                        'id': parent_segment['id'],
                        'name': parent_segment['name'],
                        'matrixDatabaseName': behavior['matrixDatabaseName'],
                        'matrixTableName': behavior['matrixTableName'],
                        'matrixColumnName': field['matrixColumnName']
                    }
                    rows.append(row)
        
        behaviours = pd.DataFrame.from_dict(rows)

        grouped = behaviours.groupby(['id', 'name', 'matrixDatabaseName', 'matrixTableName'])['matrixColumnName'].apply(list).reset_index()
        items = grouped[grouped['matrixTableName'] == 'behavior_detailed_emailactivity']

        comb_items = pd.DataFrame()

        con = td.connect(apikey=API_KEY, endpoint="https://console-next.eu01.treasuredata.com/")

        for index, item in items.iterrows():
            print(f"TREASURE DATA | PARENT SEGMENT NAME: {item['name']}")
            engine = td.create_engine(f"presto:{item['matrixDatabaseName']}", con=con)

            query=f"SELECT activity_type, asset_name, campaign_name, COUNT(email) as email FROM {item['matrixTableName']} WHERE source_system = 'Eloqua' GROUP BY activity_type, asset_name, campaign_name"
            data = td.read_td_query(query, engine, index_col=None, parse_dates=None, distributed_join=False,  params=None)
            data['parent_segment'] = item['name']
            
            comb_items = pd.concat([comb_items, data], ignore_index=True)
        
        #Pivot from rows to columns for Send, open and click
        td_data = comb_items.pivot_table(index=['parent_segment','asset_name', 'campaign_name'], columns='activity_type', values='email', aggfunc='sum').reset_index()

        # Use fillna with
        td_data = td_data.fillna(0)
        
        # Transform columns type
        td_data['Sent'] = td_data['Sent'].astype(int)
        td_data['Open'] = td_data['Open'].astype(int)
        td_data['Click'] = td_data['Click'].astype(int)
        td_data['campaign_name'] = td_data['campaign_name'].astype(str)
        td_data['asset_name'] = td_data['asset_name'].astype(str)
        
        # Rename columns
        td_data = td_data.rename(columns={'campaign_name': 'CampaignName', 'asset_name': 'emailName', 'Sent': 'totalSends', 'Open': 'totalOpens', 'Click': 'totalClickthroughs'})
        
        # Drop columns
        td_data = td_data.drop(columns=['Bounceback'])

        print('------')
        print('SAVE DATA | WRITE DATA TO CSV FILE')
        # Save data in the 
        utils.write_dataframe_to_csv(pd.DataFrame.from_dict(td_data), 'data/script_1/campaignEmailActivities_td.csv')

        print('TREASURE DATA | ENV NAME: ' + td_final_env['name'] + ' | END...')

        print('---------------------------------------------------------------------------------------------------')
        print('COMPARE DATA FROM ELOQUA AND TD PARENT SEGMENT')
                
        merged_df = pd.merge(activity_group_df, td_data, on=['emailName', 'CampaignName'], how='left')
        merged_df = merged_df.rename(columns={'totalSends_x': 'totalSends_elq', 'totalOpens_x': 'totalOpens_elq', 'totalClickthroughs_x': 'totalClickthroughs_elq', 'totalSends_y': 'totalSends_td', 'totalOpens_y': 'totalOpens_td', 'totalClickthroughs_y': 'totalClickthroughs_td'})
        
        # Use fillna with 0
        merged_df = merged_df.fillna(0)

        # Create calculated columns
        merged_df['totalSendsDifference'] = ((merged_df['totalSends_td'] - merged_df['totalSends_elq']) / merged_df['totalSends_elq']) * 100
        merged_df['totalOpensDifference'] = ((merged_df['totalOpens_td'] - merged_df['totalOpens_elq']) / merged_df['totalOpens_elq']) * 100
        merged_df['totalClickthroughsDifference'] = ((merged_df['totalClickthroughs_td'] - merged_df['totalClickthroughs_elq']) / merged_df['totalClickthroughs_elq']) * 100
        
        # Reorder columns
        merged_df = merged_df[['instance','emailGroup', 'CampaignName', 'emailName', 'totalSends_elq', 'totalSends_td', 'totalSendsDifference', 'totalOpens_elq', 'totalOpens_td','totalOpensDifference', 'totalClickthroughs_elq', 'totalClickthroughs_td', 'totalClickthroughsDifference']]
        
        #merged_group_df = merged_df.groupby(['instance', 'emailGroup'])[['totalSendsDifference', 'totalOpensDifference', 'totalClickthroughsDifference']].mean().res

        merged_group_df = merged_df.groupby(['instance', 'emailGroup']).agg({'totalSendsDifference': ['mean', utils.calc_range], 'totalOpensDifference': ['mean', utils.calc_range], 'totalClickthroughsDifference': ['mean', utils.calc_range]}).reset_index()

        merged_group_df.columns = ['instance', 'emailGroup','diff_send_avg', 'diff_send_max', 'diff_open_avg', 'diff_open_max', 'diff_click_avg', 'diff_click_max']

        
        print('------')
        print('SAVE DATA | WRITE DATA TO CSV FILE')
        utils.write_dataframe_to_csv(pd.DataFrame.from_dict(merged_df), 'data/script_1/result.csv')
        utils.write_dataframe_to_csv(pd.DataFrame.from_dict(merged_group_df), 'data/script_1/report.csv')

        # Convert items to dataframe, write the dataframe into a csv and store it in the "data folder"
        #utils.write_dataframe_to_csv(pd.DataFrame.from_dict(rows), 'data/script_1/behaviors.csv')
      
    print("END SCRIPT...")

if __name__ == "__main__":
    # sys.argv contains the command-line arguments passed to the script, 
    # with the script name at index 0 and the rest of the arguments following.
    main(sys.argv[1:])