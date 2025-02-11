import csv
import time
import serial
import re
import json
import os
from datetime import datetime

ser = serial.Serial('/dev/ttyUSB0', 9600)

def listToString(s):
 
    # initialize an empty string
    str1 = " "
 
    # return string
    return (str1.join(s))
   
def clean_json_like_string(json_like_string):
    # Replace triple quotes with single quotes
    cleaned_string = json_like_string.replace('"""', '"')
    # Remove extra spaces and fix misplaced brackets
    cleaned_string = cleaned_string.replace('[,', '[').replace(',]', ']').replace(', ,}', '}').replace(' ', '')
    # Strip surrounding curly braces
    cleaned_string = cleaned_string.strip('{}')
    # Fix JSON formatting issues
    cleaned_string = cleaned_string.replace(',]', ']').replace(',,', ',').replace(':,[', '":[').replace(':,', '":[')
    cleaned_string = cleaned_string.replace(':,', ':[').replace(',,', ',').replace(',}', '}')
    # Replace incorrectly formatted colons and commas using regex
    cleaned_string = re.sub(r'(\w):(\[)', r'"\1":\2', cleaned_string)  # Wrap keys in double quotes
    # Add curly braces to make it a valid JSON
    cleaned_string = '{' + cleaned_string + '}'
    print(cleaned_string)
    return cleaned_string

def dict_to_csv(data, filename):
    line_values = ','.join(map(str, data["line"]))

# Update the data dictionary with the concatenated string
    data["d"] = line_values
    # Open the file in write mode with UTF-8 encoding
    write_headers = not os.path.exists(filename)
    
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
          # Write headers
        if write_headers:
            writer.writerow(['Data', 'Stime'])

    # Write each key-value pair as rows
        #for key in data:
        writer.writerow([data["line"],data["stime"][0]])

current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
# Specify the CSV file path with dynamic filename
filename  = f"output_{current_datetime}.csv"
with open("cardio_datatest.csv", "r") as f:
    reader = csv.reader(f, delimiter="\t")
    for i,line in enumerate(reader):
        ct = str( time.time())
        line = listToString(line)
        print(line)
        a= "{\"line\":["+ line + "],\"stime\":["+ ct+"]}"
        ser.write(a.encode())
        ser.write(b'\r')

        cleaned_string = clean_json_like_string(a)
        json_data = json.loads(cleaned_string)
        
        dict_to_csv(json_data, filename)
    
        time.sleep(20)
        # Specify the filename
