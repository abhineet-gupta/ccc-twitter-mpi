'''
@author: Abhineet Gupta
'''
# Regular Expression library
import re
from timeit import default_timer as timer


if __name__ == '__main__':    
    startTime = timer()
    
    testFilePath = 'sampleJSON.txt'
    tinyTwitterFilePath = 'tinyTwitter.json'
    
    # Store coordinates found in file
    coordinates = []
    p = re.compile('"coordinates\\":\\[(\\d{1,3}\\.\\d+),(-\\d{1,3}\\.\\d+)]')
    
    with open(tinyTwitterFilePath, 'r', encoding='ANSI') as fin:
        for line in fin:            
            coordinates.append(p.findall(line))
        
    del coordinates[0]
    del coordinates[-1]
    print(len(coordinates))
    print(coordinates[0], coordinates[-1])

    endTime = timer()
    print(round(endTime-startTime, 2), "secs")
    
    
# Regex: \"coordinates\":\[(\d{1,3}\.\d+),(-\d{1,3}\.\d+)]
# Link: https://regex101.com/r/FMOngC/1