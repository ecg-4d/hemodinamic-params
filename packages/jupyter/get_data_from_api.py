# ---
# jupyter:
#   jupytext:
#     formats: ipynb,md
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] papermill={"duration": 0.052835, "end_time": "2020-03-08T15:34:33.764089", "exception": false, "start_time": "2020-03-08T15:34:33.711254", "status": "completed"} tags=[]
# ## Dependencies

# + papermill={"duration": 6.692203, "end_time": "2020-03-08T15:34:40.469491", "exception": false, "start_time": "2020-03-08T15:34:33.777288", "status": "completed"} tags=[]
# %%capture

# %pip install -U ray requests

# + papermill={"duration": 0.644836, "end_time": "2020-03-08T15:34:41.127733", "exception": false, "start_time": "2020-03-08T15:34:40.482897", "status": "completed"} tags=[]
import os
import ray
import datetime
import requests

# IPython tools
from IPython.display import clear_output

# + papermill={"duration": 0.815036, "end_time": "2020-03-08T15:34:41.954566", "exception": false, "start_time": "2020-03-08T15:34:41.139530", "status": "completed"} tags=[]
_ = ray.init(redis_max_memory=10**9, object_store_memory=7.8**9)

# + [markdown] papermill={"duration": 0.090414, "end_time": "2020-03-08T15:34:42.117306", "exception": false, "start_time": "2020-03-08T15:34:42.026892", "status": "completed"} tags=[]
# ## Parameters

# + papermill={"duration": 0.308537, "end_time": "2020-03-08T15:34:42.559575", "exception": false, "start_time": "2020-03-08T15:34:42.251038", "status": "completed"} tags=["parameters"]
api_url = "https://apimapa.sicor.com.co"
api_username = ""
api_password = ""

test_patient_id = 5331

pull_data_end_date = "2020-01-01"

concurrent_workers = 100
max_consecutive_error = 150
api_data_save_path = "./api-data/"
api_error_save_path = "./ERROR"

# + [markdown] papermill={"duration": 0.034861, "end_time": "2020-03-08T15:34:42.990112", "exception": false, "start_time": "2020-03-08T15:34:42.955251", "status": "completed"} tags=[]
# ## Build URLs

# + papermill={"duration": 0.089013, "end_time": "2020-03-08T15:34:43.153377", "exception": false, "start_time": "2020-03-08T15:34:43.064364", "status": "completed"} tags=[]
import urllib.parse


auth_url = urllib.parse.urljoin(api_url, "login")
map_meta_data_url = urllib.parse.urljoin(api_url, "get_mapa/") 
map_data_url = urllib.parse.urljoin(api_url, "tabla_mediciones/")
map_measure_url = urllib.parse.urljoin(api_url, "MAPA/")
map_drug_url = urllib.parse.urljoin(api_url, "medicamentos/")

pull_data_end_date = datetime.datetime.strptime(pull_data_end_date, "%Y-%m-%d")

# + [markdown] papermill={"duration": 0.088115, "end_time": "2020-03-08T15:34:43.315549", "exception": false, "start_time": "2020-03-08T15:34:43.227434", "status": "completed"} tags=[]
# ## Get latest patient Id

# + papermill={"duration": 0.122325, "end_time": "2020-03-08T15:34:43.485093", "exception": false, "start_time": "2020-03-08T15:34:43.362768", "status": "completed"} tags=[]
import glob


def get_max_patient_id(api_data_save_path):
    files = glob.glob(os.path.join(api_data_save_path, "*.json"))

    if files:
        return max([int(os.path.basename(file).split('.')[0]) for file in files])

    return 0


start_patient_id = get_max_patient_id(api_data_save_path)
start_patient_id


# + [markdown] papermill={"duration": 0.031449, "end_time": "2020-03-08T15:34:43.569648", "exception": false, "start_time": "2020-03-08T15:34:43.538199", "status": "completed"} tags=[]
# ## Get data from API

# + papermill={"duration": 0.030087, "end_time": "2020-03-08T15:34:43.625823", "exception": false, "start_time": "2020-03-08T15:34:43.595736", "status": "completed"} tags=[]
def get_api_token(url, username, password):
    """ Get authentication token to access to other API URLs

    Parameters:
        url (str): URL from where token is going te be pulled
        username (str): API username
        password (str): API password
    
    Returns:
        str: API Euthentication token
    """
    
    payload = {
        "user": username,
        "password": password,
    }
    
    response = requests.post(url, data=payload)
    response.raise_for_status()
    
    return response.json()['res']


# + papermill={"duration": 1.53881, "end_time": "2020-03-08T15:34:45.181421", "exception": false, "start_time": "2020-03-08T15:34:43.642611", "status": "completed"} tags=[]
api_token = get_api_token(auth_url, api_username, api_password)

# + papermill={"duration": 0.788874, "end_time": "2020-03-08T15:34:46.029684", "exception": false, "start_time": "2020-03-08T15:34:45.240810", "status": "completed"} tags=[]
import urllib.parse


def get_api_data(url, token, patient_id):
    """ Get data from an specific API URL
    
    Parameters:
        url (str): API url to make a GET request to get JSON data
        token (str): Authentication token needed for the request
        patient_id (int): The patient ABPM test ID 

    Returns:
        dict: JSON data from the API 
    """
    
    url = urllib.parse.urljoin(url, f"{patient_id}/")
   
    response = requests.get(
        url,
        headers = {
            "authorization": f"Bearer {token}",
        },
    )

    response.raise_for_status()
    
    return response.json()


# + papermill={"duration": 4.244094, "end_time": "2020-03-08T15:34:50.353792", "exception": false, "start_time": "2020-03-08T15:34:46.109698", "status": "completed"} tags=[]
_ = get_api_data(map_data_url, api_token, test_patient_id)
_ = get_api_data(map_measure_url, api_token, test_patient_id)
_ = get_api_data(map_drug_url, api_token, test_patient_id)
_ = get_api_data(map_meta_data_url, api_token, test_patient_id)

# + papermill={"duration": 0.289915, "end_time": "2020-03-08T15:34:50.955048", "exception": false, "start_time": "2020-03-08T15:34:50.665133", "status": "completed"} tags=[]
import urllib.parse


def get_complete_api_data(username, password, patient_id):
    """ Get data, measure and drugs for an specific ABPM test
    
    Parameters:
        username (str): API user name used to get API token
        password (str): API password used to get API token
        patient_id (int): Patient ABPM test ID to pull data from

    Returns:
        dict: JSON data pulled from API, id has the ABPM test ID.
        Data contains the test meta data like start date, night 
        time and other importante data. Measure contains the real ABPM
        measurements. Drugs contain the drugs taken by a patient during
        the ABPM test.
    """
    
    token = get_api_token(auth_url, username, password)
    
    map_data = get_api_data(map_data_url, token, patient_id)
    map_measure = get_api_data(map_measure_url, token, patient_id)
    map_drugs =  get_api_data(map_drug_url, token, patient_id)
    map_meta_data = get_api_data(map_meta_data_url, api_token, patient_id)
    
    return {
        "id": patient_id,
        "data": map_data,
        "meta_data": map_meta_data,
        "measure": map_measure,
        "drugs": map_measure,
    }


# + papermill={"duration": 3.259291, "end_time": "2020-03-08T15:34:54.655911", "exception": false, "start_time": "2020-03-08T15:34:51.396620", "status": "completed"} tags=[]
_ = get_complete_api_data(api_username, api_password, test_patient_id)


# + [markdown] papermill={"duration": 0.028144, "end_time": "2020-03-08T15:34:54.746819", "exception": false, "start_time": "2020-03-08T15:34:54.718675", "status": "completed"} tags=[]
# ## Parallelized API data collection

# + papermill={"duration": 0.020464, "end_time": "2020-03-08T15:34:54.783255", "exception": false, "start_time": "2020-03-08T15:34:54.762791", "status": "completed"} tags=[]
@ray.remote
def get_complete_api_data_async(username, password, patient_id):
    """ Wrapper for the get_complete_api_data so that it can be ran
    in parallel and asyncrhonusly 
    
    Parameters:
        username (str): API user name used to get API token
        password (str): API password used to get API token
        patient_id (int): Patient ABPM test ID to pull data from

    Returns:
        dict: JSON data pulled from API, id has the ABPM test ID.
        Data contains the test meta data like start date, night 
        time and other importante data. Measure contains the real ABPM
        measurements. Drugs contain the drugs taken by a patient during
        the ABPM test.
    """
    
    try:
        user_data = get_complete_api_data(username, password, patient_id)
    except requests.HTTPError as error:
        with open(api_error_save_path, "a+") as file:
            error_data = f"ERROR -- Patient ID: {patient_id} -- {error.response.status_code} \n"
            file.write(error_data)
            
        return None
    
    return user_data


# + papermill={"duration": 4.267014, "end_time": "2020-03-08T15:34:59.062874", "exception": false, "start_time": "2020-03-08T15:34:54.795860", "status": "completed"} tags=[]
_ = ray.get(get_complete_api_data_async.remote(api_username, api_password, test_patient_id))


# + [markdown] papermill={"duration": 0.897492, "end_time": "2020-03-08T15:34:59.999325", "exception": false, "start_time": "2020-03-08T15:34:59.101833", "status": "completed"} tags=[]
# ## Stop condition for API data pull

# + papermill={"duration": 0.027734, "end_time": "2020-03-08T15:35:00.100908", "exception": false, "start_time": "2020-03-08T15:35:00.073174", "status": "completed"} tags=[]
def list_contains_date_grater_than(patient_data_list, end_date):
    """
    
    Parameters:
        patient_data_list (dict): List of ABPM request resonses
        end_date (datetime): The newest date for the revelant data
        we are pretending to get.

    Returns:
        bool: If the end_data is less than any of the start_dates of
        the ABPM return True so that pulling data ends. Other way
        return False so that data pull continues.
    """
    
    for map_data in patient_data_list:   
        if map_data and map_data.get('data'):  
            start_date = map_data['data'][0]['fecha_dt'].split(' ')[0]
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        else:
            start_date = datetime.datetime.strptime("1900-1-1", '%Y-%m-%d')
                   
        if end_date < start_date:
            return True
        
    return False


# + papermill={"duration": 0.14055, "end_time": "2020-03-08T15:35:00.254233", "exception": false, "start_time": "2020-03-08T15:35:00.113683", "status": "completed"} tags=[]
# Test the list_contains_date_grater_than function

assert list_contains_date_grater_than(
    [{
        'data': [
            {
                'fecha_dt': "2015-1-1 03:55:21"
            }
        ]
    }],     
    
    datetime.datetime.strptime("2011-1-1", '%Y-%m-%d')
) == True

# + papermill={"duration": 0.023659, "end_time": "2020-03-08T15:35:00.325835", "exception": false, "start_time": "2020-03-08T15:35:00.302176", "status": "completed"} tags=[]
# Test the list_contains_date_grater_than function

assert list_contains_date_grater_than(
    [{
        'data': [
            {
                'fecha_dt': "2011-1-1 03:55:21"
            }
        ]
    }],     
    
    datetime.datetime.strptime("2015-1-1", '%Y-%m-%d')
) == False


# + [markdown] papermill={"duration": 0.014364, "end_time": "2020-03-08T15:35:00.355063", "exception": false, "start_time": "2020-03-08T15:35:00.340699", "status": "completed"} tags=[]
# ## Save the data from the API

# + papermill={"duration": 0.104463, "end_time": "2020-03-08T15:35:00.474799", "exception": false, "start_time": "2020-03-08T15:35:00.370336", "status": "completed"} tags=[]
def pull_api_data(start_patient_id, pull_data_end_date, concurrent_workers, max_consecutive_error):
    """ Get data from API for all patients in the range start_patient_id to the first
    patient_id whos start_date < pull_data_end_date.
    
    Parameters:
        start_patient_id (int): The firts ABPM test ID from where to start pulling data
        pull_data_end_date (datetime): The upper date cap for test to be pulled
        concurrent_workers (int): The number of concurrent requests to be made at a single time
        max_consecutive_error (int): The maximum number of continous errors before stoping the pulling process

    Returns:
        generator: Generates an API response for each of the users pulled at batches of size concurrent_workers
    """
    
    error_count = 0
    finish_pulling = False
    start_pull_index = start_patient_id

    while not finish_pulling:
        end_pull_index = start_pull_index + concurrent_workers
        
        futures = [
            get_complete_api_data_async.remote(
                api_username,
                api_password,
                patient_id
            ) for patient_id in range(start_pull_index, end_pull_index)
        ]
        
        api_data = ray.get(futures)
        
        finish_pulling = list_contains_date_grater_than(api_data, pull_data_end_date)
        
        if finish_pulling: 
            break
        
        for id, patient_data in enumerate(api_data):
            if patient_data and patient_data.get("data"):
                error_count = 0
                yield patient_data
            else:
                error_count += 1
                
        if max_consecutive_error < error_count:
            finish_pulling = True
            
        start_pull_index = end_pull_index


# + papermill={"duration": 186.400283, "end_time": "2020-03-08T15:38:06.912773", "exception": false, "start_time": "2020-03-08T15:35:00.512490", "status": "completed"} tags=[]
import json
import time


start_time = time.time()
os.makedirs(api_data_save_path, exist_ok=True)

for index, patient_data in enumerate(pull_api_data(start_patient_id, pull_data_end_date, concurrent_workers, max_consecutive_error)):
    elapsed_time = time.time() - start_time

    with open(os.path.join(api_data_save_path, f"{patient_data.get('id')}.json"), "w+") as file:
        file.write(json.dumps(patient_data))
            
    clear_output(wait=True)
    print(f"Speed: {index / elapsed_time}r/s -- Elapse Time: {elapsed_time}s -- Patient Id: {patient_data.get('id')}")

# + [markdown] papermill={"duration": 0.050278, "end_time": "2020-03-08T15:38:06.980527", "exception": false, "start_time": "2020-03-08T15:38:06.930249", "status": "completed"} tags=[]
# ## API Errors

# + papermill={"duration": 0.02603, "end_time": "2020-03-08T15:38:07.050131", "exception": false, "start_time": "2020-03-08T15:38:07.024101", "status": "completed"} tags=[]
import os


if os.path.exists(api_error_save_path):
    with open(api_error_save_path, "r+") as file:
        print(file.read())
# -


