# ---
# jupyter:
#   jupytext:
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

# +
# %%capture

# %pip install pandas seaborn
# %matplotlib inline
# -

api_data_path = "./api-data/"
base_dataset_path = "./sleep_dataset.csv"

# +
import os
import glob
import json
import seaborn as sns

patient_files = glob.glob(os.path.join(api_data_path, "*.json"))
patient_data = [
    json.loads(open(file, "r").read()) for file in patient_files
]

# +
import pandas

data = []

for patient in patient_data:    
    patient_id = patient["id"]

    birth_date = patient["meta_data"][0]["fecha_nacimiento"]
    start_date = patient["meta_data"][0]["fecha_inicio"]

    start_night = patient["meta_data"][0]["inicio_noche"]
    end_night = patient["meta_data"][0]["fin_noche"]

    gender = patient["meta_data"][0]["genero"]
    height = patient["meta_data"][0]["talla"]
    weight = patient["meta_data"][0]["peso"]

    data.append([patient_id, birth_date, start_date, start_night, end_night, gender, height, weight])
    

meta_data_df = pandas.DataFrame(
    data, 
    columns=[
        "patient_id", 
        "birth_date", 
        "start_date", 
        "start_night", 
        "end_night", 
        "gender", 
        "height", 
        "weight"
    ]
)

meta_data_df.set_index("patient_id", inplace=True)
meta_data_df

# +
data = []

for patient in patient_data:   
    patient_id = patient["id"]
    
    for measure in patient["data"]:
        measure_date_time = measure["fecha_dt"]
        sistolic = measure["sistolica"]
        diastolic = measure["diastolica"]
        heart_reate = measure["valor"]
        
        data.append([patient_id, measure_date_time, sistolic, diastolic, heart_reate])
        
        
measure_df = pandas.DataFrame(
    data, 
    columns=[
        "patient_id",
        "measure_date_time", 
        "sistolic", 
        "diastolic", 
        "heart_reate",
    ]
)

measure_df.set_index("patient_id", inplace=True)
measure_df

# +
patient_df = pandas.merge(
    meta_data_df, 
    measure_df, 
    how="inner", 
    left_index=True, 
    right_index=True
)

patient_df
# -

patient_df.to_csv(sleep_dataset_path)


