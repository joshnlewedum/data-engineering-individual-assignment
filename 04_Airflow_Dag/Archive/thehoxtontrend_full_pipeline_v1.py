# Importing Airflow Functions
from airflow import DAG
from airflow.models import BaseOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks import aws_lambda_hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from datetime import datetime
from datetime import timedelta
import time
import os

import logging

log = logging.getLogger(__name__)

# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================

default_args = {
    'start_date': datetime(2021, 5, 9),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    
    'db_name': Variable.get("thehoxtontrend", deserialize_json=True)['db_name'],
    'api_key': Variable.get("thehoxtontrend", deserialize_json=True)['api_key'],
    'bucket_name': Variable.get("thehoxtontrend", deserialize_json=True)['bucket_name'],
    'description_table_key': Variable.get("thehoxtontrend", deserialize_json=True)['description_table_key'] + time.strftime("%Y_%m_%d") + "_" + str(int(datetime.now().timestamp())) + '.csv',
    'statistics_table_key': Variable.get("thehoxtontrend", deserialize_json=True)['statistics_table_key'] + time.strftime("%Y_%m_%d") + "_" + str(int(datetime.now().timestamp())) + '.csv',
    'comments_table_key' : Variable.get("thehoxtontrend", deserialize_json=True)['comments_table_key'] + time.strftime("%Y_%m_%d") + "_" + str(int(datetime.now().timestamp())) + '.csv',
    'tags_table_key' : Variable.get("thehoxtontrend", deserialize_json=True)['tags_table_key'] + time.strftime("%Y_%m_%d") + "_" + str(int(datetime.now().timestamp())) + '.csv',
    'google_analytics_table_key' : Variable.get("thehoxtontrend", deserialize_json=True)['google_analytics_table_key'] + '.csv',
    'aws_conn_id': 'aws_default_chigozienlewedum',
    'postgres_conn_id': 'postgres_conn_id_chigozienlewedum',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('thehoxtontrend_data_full_pipeline',
          description="""Collecting TheHoxtonTrend's YouTube Channel Data including, Video Descriptions,
          Video Statistics, Video Comments and Video Tags via the YouTube Data API. Then pulling Historical
          Website Metrics from a Flat File in AWS S3. Storing Data within Postgres SQL database. Taking
          relevant sections of data to perform linear regression model with AWS Lambda and deploying results
          via AWS Gateway API""",
          schedule_interval='@weekly', 
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================

def youtube_scraping_function(**kwargs):
    
    import requests
    import json
    import pandas as pd
    
    bucket_name = kwargs['bucket_name']
    api_key = kwargs['api_key']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    # Read the content of the key from the bucket
    key_json = s3.read_key(api_key, bucket_name)

    def youtube_search_query():
        #Build URL
        url = 'https://youtube.googleapis.com/youtube/v3/search?part=snippet&channelId=UCFFDDhKEm6nyFWV8WncAOkw&maxResults=500&order=date&publishedAfter=2017-11-01T00%3A00%3A00Z&type=video&key='+key_json
    
        #Request URL
        r = requests.get(url)
        
        #Load JSON data from webpage into data variable
        data = json.loads(r.text)
        
        all_responses = []
        
        while data:
            all_responses.append(data)
            if 'nextPageToken' in data:
                page_token = data['nextPageToken']
                
                url = 'https://youtube.googleapis.com/youtube/v3/search?part=snippet&channelId=UCFFDDhKEm6nyFWV8WncAOkw&maxResults=500&order=date&pageToken='+page_token+'&publishedAfter=2017-11-01T00%3A00%3A00Z&type=video&key='+key_json
                r = requests.get(url)
                data = json.loads(r.text)
                
            else:
                break
   
        #return the data element which contains all the submissions data
        return all_responses
    
    all_search_results = youtube_search_query()
    
    # # Setting configuration for Search API Query
    # config = {"part":"snippet",
    #           "channelId":"UCFFDDhKEm6nyFWV8WncAOkw",
    #           "maxResults":500,
    #           "order":"date",
    #           "publishedAfter":"2017-11-01T00:00:00Z",
    #           "type":"video"}
    
    # youtube_hook = GoogleDiscoveryApiHook(api_service_name="youtube",
    #                                       api_version="v3",
    #                                       gcp_conn_id = 'google_cloud_default')
    
    # connection = youtube_hook.get_conn()
    # all_responses = connection.query(endpoint = "youtube.search.list",
    #                             data = config,
    #                             paginate = True,
    #                             num_retries = 0)
    
    # Installing the relevant packages
    # !pip install google-api-python-client
    
    # # Importing the relevant packages
    # import os
    # import googleapiclient.discovery
    # import pandas as pd
    
    # api_key = kwargs['api_key']

    
    # # Empty list for the results of the Youtube Search API Query
    # all_responses = []
    
    # log.info('Going to scrape description (search) data from the YouTube API')
    
    # # Function for the Youtube Search API Query
    # def main():
    #     # Disable OAuthlib's HTTPS verification when running locally.
    #     # *DO NOT* leave this option enabled in production.
    #     # os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
    #     api_service_name = "youtube"
    #     api_version = "v3"
    #     DEVELOPER_KEY = api_key
    
    #     # Get credentials and create an API client
    #     youtube = googleapiclient.discovery.build(
    #         api_service_name, api_version, developerKey = DEVELOPER_KEY)
    
    #     request = youtube.search().list(**config)
    #     response = request.execute()
        
    #     while response:
    #         all_responses.append(response)
    #         if 'nextPageToken' in response:
    #             config['pageToken'] = response['nextPageToken']
    #             response = youtube.search().list(**config).execute()
    #             break    
    
    # if __name__ == "__main__":
    #      main()
    
    log.info('Successfully scraped description (search) data from the YouTube API')     
    
    # Empty Lists which will form the Columns of the Video Description Dataframe
    upload_date = []
    video_id = []
    video_title = []
    video_description = []
    
    # Looping through the JSON response to append values to empty column lists
    for level in range(len(all_search_results)):
        for sub_level in range(len(all_search_results[level]['items'])):
            upload_date.append(all_search_results[level]['items'][sub_level]['snippet']['publishedAt'])
            video_id.append(all_search_results[level]['items'][sub_level]['id']['videoId'])
            video_title.append(all_search_results[level]['items'][sub_level]['snippet']['title'])
            video_description.append(all_search_results[level]['items'][sub_level]['snippet']['description'])
            
    # Creating the Video Description Dataframe       
    video_description_df = pd.DataFrame(list(zip(upload_date, video_id, video_title, video_description)),
                                 columns =['Date', 'Video ID','Video Title', 'Video Description'])
    
    log.info('Successfully processed description (search) data from the YouTube API')
    
    # Defining function to extract information from the comment
    def extract_infos_from_comment(comment,fields=["textOriginal"]):
        snippet = comment.get("snippet")
        if(snippet):
            return( {key:snippet.get(key) for key in fields})
        else:
            return(None)
    
    # Defining function to get comment from raw result
    def get_comment_from_raw_result(result,fields=["textOriginal"]):
        main_comment = None
        replies = []
        snippet = result.get("snippet")
        if(snippet):
            top_level_comment = snippet.get("topLevelComment")
            if(top_level_comment):
                main_comment = extract_infos_from_comment(top_level_comment,fields=fields)
            else:
                pass
        else:
            pass
        list_replies = result.get("replies")
        if(list_replies):
            comments = list_replies.get("comments")
            if(comments):
                for comment in comments:
                    replies.append(extract_infos_from_comment(comment,fields=fields))
        dic = {"Main_comment":main_comment,"replies":replies}            
        return(dic)
    
    # Defining a function to get all comments from results
    def get_all_comments_from_results(results,fields=["textOriginal"]):
        items = results.get("items")
        all_comments = [get_comment_from_raw_result(item,fields=fields) for item in items]
        return(all_comments)
    
    log.info('Going to scrape comment data from the YouTube API')    
    
    # Defining a function to get all comments
    def get_all_comments(config_request,fields=["textOriginal"],verbose=False):
        all_comments = []
        
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
        api_service_name = "youtube"
        api_version = "v3"
        DEVELOPER_KEY = api_key
    
        # Get credentials and create an API client
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey = DEVELOPER_KEY)
        
        results = youtube.commentThreads().list(**config_request).execute()
        current_page = 0
        n_total_comments = 0
        while results:
            current_page += 1
            if( verbose):
                print("parsing comments for page {}..".format(current_page))
            comments_this_page = get_all_comments_from_results(results,fields=fields)
            n_comments = len(comments_this_page)
            n_total_comments += n_comments
            if(verbose):
                print("Found {} comments on this page..".format(n_comments))
            all_comments.append(comments_this_page)
            if 'nextPageToken' in results:
                        config_request['pageToken'] = results['nextPageToken']
                        results = youtube.commentThreads().list(**config_request).execute()
            else:
                break
        dic = {"all_comments":all_comments,"n_comments":n_total_comments}
        return(dic)

    log.info('Successfully scraped comment data from the YouTube API') 
    
    # Empty Lists which will form the Columns of the Video Comments Dataframe
    comments_temp = []
    comment_time_temp = []
    video_id_temp = []
    comment_author_temp = []
    like_count_temp = []
    reply_flag_temp = []
    
    # Looping through all videos in the Video Description Dataframe
    for video in video_id:
        
        # Pulling comments from a selected video
        config_request = {"part":"id,snippet,replies",
                          "order":"time",
                          "videoId": video}
    
        fields = ["textOriginal","publishedAt",'videoId','authorDisplayName','likeCount']
    
        all_comments = get_all_comments(config_request,fields=fields,verbose=True)
    
        # Looping through the JSON response to append values to empty column lists
        for full_list in all_comments["all_comments"]:
            for element in full_list:
                for j in element:
                    if j == "Main_comment":
                        comments_temp.append(element["Main_comment"]["textOriginal"])
                        comment_time_temp.append(element["Main_comment"]["publishedAt"])
                        video_id_temp.append(element["Main_comment"]["videoId"])
                        comment_author_temp.append(element["Main_comment"]["authorDisplayName"])
                        like_count_temp.append(element["Main_comment"]["likeCount"])
                        reply_flag_temp.append(0)
                    elif j  == "replies":
                        for reply in element["replies"]:
                            comments_temp.append(reply["textOriginal"])
                            comment_time_temp.append(reply["publishedAt"])
                            video_id_temp.append(reply["videoId"])
                            comment_author_temp.append(reply["authorDisplayName"])
                            like_count_temp.append(reply["likeCount"])
                            reply_flag_temp.append(1)
                            
    # Creating Video Comments dataframe
    video_comments_df = pd.DataFrame({'Date': comment_time_temp,
                                      'Video ID': video_id_temp,
                                      'Comment': comments_temp,
                                      'Author': comment_author_temp,
                                      'Likes': like_count_temp,
                                      'Reply flag': reply_flag_temp})
    
    video_comments_df = video_comments_df.to_json()
    
    log.info('Successfully processed comments data from the YouTube API')
    
    # Empty list for the results of the Youtube Video API Query
    all_statistics_info = []
    
    log.info('Going to scrape statistics data from the YouTube API') 
    
    # Function for the Youtube Video (Statistics-specific) API Query
    def main():
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        # os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
        api_service_name = "youtube"
        api_version = "v3"
        DEVELOPER_KEY = api_key
    
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey = DEVELOPER_KEY)
        
        # Looping through all videos in the Video Description Dataframe
        for video in video_id:
    
            request = youtube.videos().list(
                part="statistics",
                id=video
            )
            response = request.execute()
    
            all_statistics_info.append(response)
    
    if __name__ == "__main__":
        main()
    
    log.info('Successfully scraped statistics data from the YouTube API')     
    
    # Empty Lists which will form the Columns of the Video Statistics Dataframe
    views = []
    likes = []
    dislikes = []
    comments_count = []
    
    # Looping through the JSON response to append values to empty column lists
    for level in range(len(all_statistics_info)):
        views.append(all_statistics_info[level]['items'][0]['statistics']['viewCount'])
        likes.append(all_statistics_info[level]['items'][0]['statistics']['likeCount'])
        dislikes.append(all_statistics_info[level]['items'][0]['statistics']['dislikeCount'])
        comments_count.append(all_statistics_info[level]['items'][0]['statistics']['commentCount'])
    
    # Creating the Video Statistics Dataframe    
    video_statistics_df = pd.DataFrame(list(zip(video_id, views, likes, dislikes, comments_count)),
                                       columns =['Video ID','Views', 'Likes', 'Dislikes', 'Comments'])
    
    video_statistics_df = video_statistics_df.to_json()
    
    log.info('Successfully processed statistics data from the YouTube API')
    
    # Empty list for the results of the Youtube Video API Query
    all_tags = []
    
    log.info('Going to scrape tag data from the YouTube API') 
    
    # Function for the Youtube Video (Snippet-specifc) API Query
    def main():
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
        api_service_name = "youtube"
        api_version = "v3"
        DEVELOPER_KEY = api_key
    
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey = DEVELOPER_KEY)
        
        # Looping through all videos in the Video Description Dataframe
        for video in video_id:
    
            request = youtube.videos().list(
                part="snippet",
                id=video
            )
            response = request.execute()
    
            all_tags.append(response)
    
    if __name__ == "__main__":
        main()
    
    log.info('Successfully scraped tag data from the YouTube API')
    
    # Empty Lists which will form the Tag-related Columns of the Video Comments Dataframe
    # and will replace the description column in the Video Description Dataframe
    # orginal description column does not contain full video descriptions
    tags = []
    full_descriptions = []
    
    # Looping through the JSON response to append values to empty column lists
    for level in range(len(all_tags)):
        # Appending blank values in cases where a tag is not present 
        # as a tag was not present for the video
        try:
            tags.append(all_tags[level]['items'][0]['snippet']['tags'])
        except KeyError as e:
            tags.append([""])
        full_descriptions.append(all_tags[level]['items'][0]['snippet']['description'])
    
    # Creating Video Tags Dataframe
    video_tags_df = pd.DataFrame(tags)
    
    # Creating column names for Video Tags Dataframe
    column_names = [*range(1, video_tags_df.shape[1]+1)]
    video_tags_df.columns = column_names
    
    # Adding Video ID Column to Video Tags Dataframe
    video_tags_df["Video ID"] = video_id
    
    # Restructing Video Tags Dataframe (Unpivotting, Removing Rows with NAs, Sorting Rows)
    video_tags_df = video_tags_df.melt(id_vars=['Video ID'], var_name='Tag Number', value_name='Tag Value')
    video_tags_df.dropna(axis=0, inplace=True)
    video_tags_df.sort_values(by=["Video ID", "Tag Number"], ascending=[False, True], inplace=True)
    
    video_tags_df = video_tags_df.to_json()
    
    log.info('Successfully processed tag data from the YouTube API')
    
    # Replacing video description column in Video Description Dataframe
    video_description_df["Video Description"] = full_descriptions
    
    video_description_df= video_description_df.to_json()
    
    log.info('Successfully updated description data from the YouTube API')
    
    return video_description_df, video_statistics_df, video_comments_df, video_tags_df
    
    
def s3_save_file_func(**kwargs):

    import pandas as pd
    import io

    bucket_name = kwargs['bucket_name']
    description_table_key = kwargs['description_table_key']
    statistics_table_key  = kwargs['statistics_table_key']
    comments_table_key  = kwargs['comments_table_key']
    tags_table_key  = kwargs['tags_table_key']    
    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the bash task
    scraped_data_previous_task = task_instance.xcom_pull(task_ids="youtube_web_scraping_task")

    log.info('xcom from youtube_web_scraping_task:{0}'.format(scraped_data_previous_task))

    # Load the list of dictionaries with the scraped data from the previous task into a pandas dataframe
    log.info('Loading scraped data into pandas dataframe')
    
    
    video_description_df = pd.DataFrame.from_dict(eval(scraped_data_previous_task[0]))
    video_statistics_df = pd.DataFrame.from_dict(eval(scraped_data_previous_task[1]))
    video_comments_df = pd.DataFrame.from_dict(eval(scraped_data_previous_task[2]))
    video_tags_df = pd.DataFrame.from_dict(eval(scraped_data_previous_task[3]))

    log.info('Saving scraped data to S3')

    # Prepare the file to send to s3
    description_csv_buffer = io.StringIO()
    video_description_df.to_csv(description_csv_buffer, index=False)
    
    statistics_csv_buffer = io.StringIO()
    video_statistics_df.to_csv(statistics_csv_buffer, index=False)
    
    comments_csv_buffer = io.StringIO()
    video_comments_df.to_csv(comments_csv_buffer, index=False)
    
    tags_csv_buffer = io.StringIO()
    video_tags_df.to_csv(tags_csv_buffer, index=False)

    
    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')
    
    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    description_data = description_csv_buffer.getvalue()
    statistics_data = statistics_csv_buffer.getvalue()
    comments_data = comments_csv_buffer.getvalue()
    tags_data = tags_csv_buffer.getvalue()

    print("Saving CSV files")
    description_object = s3.Object(bucket_name, description_table_key)
    statistics_object = s3.Object(bucket_name, statistics_table_key)
    comments_object = s3.Object(bucket_name, comments_table_key)
    tags_object = s3.Object(bucket_name, tags_table_key)

    # Write the file to S3 bucket in specific path defined in key
    description_object.put(Body=description_data)
    statistics_object.put(Body=statistics_data)
    comments_object.put(Body=comments_data)
    tags_object.put(Body=tags_data)

    log.info('Finished saving the scraped data to s3')



# =============================================================================
# 3. Set up the main configurations of the dags
# =============================================================================


youtube_web_scraping_task = PythonOperator(
    task_id='youtube_web_scraping_task',
    provide_context=True,
    python_callable=youtube_scraping_function,
    op_kwargs=default_args,
    dag=dag,

)

save_scraped_data_to_s3_task = PythonOperator(
    task_id='save_scraped_data_to_s3_task',
    provide_context=True,
    python_callable=s3_save_file_func,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)


# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

youtube_web_scraping_task >> save_scraped_data_to_s3_task
