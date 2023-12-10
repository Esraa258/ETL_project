# import libraries
import glob 
import pandas as pd 
# import xml.etree library to parse the data from an XML file format
import xml.etree.ElementTree as ET 
from datetime import datetime

# transformed_data.csv --> store the final output data that you can load to a database,
# log_file.txt --> stores all the logs.
log_file = "log_file.txt" 
target_file = "transformed_data.csv" 


###########################################################################################################################
####################################################### EXTRACTION ########################################################
###########################################################################################################################
# extract data from different file formats

# extract data from csv file
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

# extract data from json file
# lines=True --> to enable the function to read the file as a JSON object on line to line basis
def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe 

# extract data from XML file
# parse the data from the file using the ElementTree function
# then extract relevant information from this data and append it to a pandas dataframe
def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return dataframe


# create a function to identify which function to call on basis of the filetype of the data file. 
# To call the relevant function, write a function extract, which uses the glob library to identify the filetype.

# The function extract: will extract large amount of data from multiple sources in batches
def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data 

    # use the glob function from the glob module
    # the input is the file extension, the output is a list of files with that particualr extension

    #adding a loop to find all the csv files
    # by adding the glob function, it will find and load all of the csv file names, 
    # and with each iteration of the loop the csv files are appended to the dataframe.

    # ignore_index=True ---> the order of each row would be the same as the order the row was appended to the dataframe.

    # process all csv files 
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 


###########################################################################################################################
###################################################### TRANSFORMATION #####################################################
###########################################################################################################################
# Since the dataframe is in the form of a dictionary with three keys, "name", "height", and "weight", 
# each of them having a list of values, you can apply the transform function on the entire list at one go.
def transform(data): 
    #Convert inches to meters and round off to two decimals (1 inch is 0.0254 meters)
    data['height'] = round(data.height * 0.0254,2) 
 
    #Convert pounds to kilograms and round off to two decimals (1 pound is 0.45359237 kilograms)
    data['weight'] = round(data.weight * 0.45359237,2) 
    
    return data
# The output of this function is a dataframe where the "height" and "weight" parameters will be modified to the required format.


###########################################################################################################################
################################################### Loading and Logging ###################################################
###########################################################################################################################
# load the transformed data to a CSV file that you can use to load to a database as per requirement.
# To load the data, you need a function load_data() that accepts the transformed data as a dataframe and the target_file path.
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file)

# implement the logging operation to record the progress of the different operations. 
# For this operation, you need to record a message, along with its timestamp, in the log_file
# To record the message, you need to implement a function log_progress() that accepts the log message as the argument.
# The function captures the current date and time using the datetime function from the datetime library.
# The use of this function requires the definition of a date-time format, and you need to convert the timestamp to a string format using the strftime attribute.

#create a logging function
def log_progress(message):
    #timestamp_format --> determines the formatting of the time &date
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    #now --> capture the current time by calling datetime
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    #pull that information together (by opening a file &writing the information to the file)
    # 'a' ---> all the data written will be appended to the existing information
    with open(log_file,"a") as f:
        #we are then able to attach a timestamp to each part of the process of when it begins &when it has completed
        f.write(timestamp + ',' + message + '\n') 


###########################################################################################################################
################################################## Testing ETL operations and log progress ################################
###########################################################################################################################
##### CALL ALL THE FUNCTIONS ######

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 

#call the extract_data function
#(The data received from this step will then be transferred to the 2nd step of transforming, after this has completed the data
# is then loaded into the target file)
#before & after every step the time and date for start and completion has been added

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