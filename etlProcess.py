import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "D:/NNP/ETL/log_file.txt"
target_file = "D:/NNP/ETL/transformed_data.csv"

#Function to extract the data (Extract)

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    print(f"Extracted CSV data from {file_to_process}: {dataframe.head()}")
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    print(f"Extracted json data from {file_to_process}: {dataframe.head()}")
    return dataframe

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["VitA", "firstDose", "secondDose"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for supp in root: 
        VitA = supp.find("VitA").text 
        firstDose = float(supp.find("firstDode").text) 
        secodDose = float(supp.find("secondDose").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"VitA":VitA, "firstDose":firstDose, "secondDose":secodDose}])], ignore_index=True) 
    print(f"Extracted xml data from {file_to_process}: {dataframe.head()}")
    return dataframe 

def extract(): 
    extracted_data = pd.DataFrame(columns=['VitA','firstDose','secondDose']) # create an empty data frame to hold extracted data 
     
    # process all csv files, except the target file
    for csvfile in glob.glob("D:/NNP/ETL/*.csv"): 
        if csvfile != target_file:  # check if the file is not the target file
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    # process all json files 
    for jsonfile in glob.glob("D:/NNP/ETL/*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("D:/NNP/ETL/*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 




def transform(data): 
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data.height * 0.0254,2) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2) 
    
    return data 
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 
