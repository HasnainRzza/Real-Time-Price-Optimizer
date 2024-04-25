#!/usr/bin/env python
# coding: utf-8

import zipfile
import json

# Path to your zip file
zip_file_path = "Amazon_Meta.zip"

def read_data_from_zip(zip_file_path):
    data = {}
    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        for name in zip_file.namelist():
            data[name] = []
            with zip_file.open(name) as file:
                while True:
                    line = file.readline().decode('utf-8')
                    if not line:
                        break
                    data[name].append(json.loads(line))
    return data

data = read_data_from_zip(zip_file_path)

act_data = []
for file_name, lines in data.items():
    #print(f"First 10 lines of {file_name}:")
    for line in lines:
        act_data.append(line)

less_relevant_keys = ['category','title', 'image', 'details', 'similar_item', 'also_view', 'tech1', 'tech2','fit', 'rank', 'main_cat', 'date', 'feature','description']


for key in less_relevant_keys:      
    for i,data in enumerate(act_data):
        if key in act_data[i]:
            del act_data[i][key]


act_data_keys=list(act_data[0].keys())
act_data_keys

for key in act_data_keys:
    for data in act_data:
        if key in data and isinstance(data[key], list):  
            data[key] = [word.lower() for word in data[key]]
        elif key in data and isinstance(data[key], str): 
            data[key] = data[key].lower()


output_path = "processed_sample.json"

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(act_data, f, ensure_ascii=False, indent=4)

print(f"Done")
