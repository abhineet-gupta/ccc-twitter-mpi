'''
@author: Abhineet Gupta
CCC Assignment 1; Sem 1, 2017
Mapping Twitter Data
'''

import re   #regex library
from timeit import default_timer as timer
import operator

TwitterFilePath = 'smallTwitter.json'
gridFilePath = 'melbGrid.json'

# Regex for matching tweets_coordinates in JSON file for each tweet
coord_patt = re.compile('\\"coordinates\\":\\[(14[45]\\.\\d+),(-3[78]\\.\\d+)]')
# Regex for gridbox coordinates
grid_patt = re.compile('\\"([A-Z]\\d)\\", \\"xmin\\": (\\d{1,3}\\.\\d+), \\"xmax\\": (\\d{1,3}\\.\\d+), \\"ymin\\": (-\\d{1,3}\\.\\d+), \\"ymax\\": (-\\d{1,3}\\.\\d+)')

box_counts = {}
tweets_read = -1
tweets_categorised = 0

def readJSONTweetFile(filePath, coordinates = []):
    global tweets_read
    
    # Read file line by line
    with open(filePath, 'r', encoding='ansi') as fin:
        for line in fin:
            tweets_read += 1    
            temp_res = coord_patt.findall(line)
            if (temp_res): # if results are not null
    #             Append coordinates found into list, as a tuple of floats       
                coordinates.append((float(temp_res[0][0]), float(temp_res[0][1])))
    
    # list of (longitude, latitude) tuples
    return coordinates 

def readJSONGridboxFile(filepath, gridboxes = []):
    global box_counts
    
    with open(filepath, 'r') as fin:
        for line in fin:
            temp_box = grid_patt.findall(line)
            if (temp_box):
                gridboxes.append((temp_box[0][0], float(temp_box[0][1]), float(temp_box[0][2]), float(temp_box[0][3]), float(temp_box[0][4])))
                box_counts[temp_box[0][0]] = 0
    
    # list of (ID, xmin, xmax, ymin, ymax) tuples
    return gridboxes

def gridifyTweets(tw_coord, _grid_boxes):
    global tweets_categorised, box_counts
    
    for tweet_loc in tw_coord:        
        for grid_box in _grid_boxes:
            if tweet_loc[0] > grid_box[1] and tweet_loc[0] <= grid_box[2] \
            and tweet_loc[1] >= grid_box[3] and tweet_loc[1] < grid_box[4]:
                box_counts[grid_box[0]]+=1
                tweets_categorised+=1

def tallyTweetsbyRowCol(row_counts, col_counts):
    for zone in box_counts:
        if zone[0] == 'A':
            row_counts[zone[0]] += box_counts[zone]
        elif zone[0] == 'B':
            row_counts[zone[0]] += box_counts[zone]
        elif zone[0] == 'C':
            row_counts[zone[0]] += box_counts[zone]
        elif zone[0] == 'D':
            row_counts[zone[0]] += box_counts[zone] 
            
        if zone[1] == '1':
            col_counts[zone[1]] += box_counts[zone]
        elif zone[1] == '2':
            col_counts[zone[1]] += box_counts[zone]
        elif zone[1] == '3':
            col_counts[zone[1]] += box_counts[zone]
        elif zone[1] == '4':
            col_counts[zone[1]] += box_counts[zone]
        elif zone[1] == '5':
            col_counts[zone[1]] += box_counts[zone]    
    
def printStats(sorted_cols, sorted_rows, sorted_zones):
    print("")
    for zone in sorted_zones:
        print(zone[0] + ":", zone[1], "tweets.")
    print("")            
    for row in sorted_rows:
        print(row[0] + "-Rows:", row[1], "tweets.")
    print("")
    for col in sorted_cols:
        print("Column", col[0] + ":", col[1], "tweets.")
    print("")
    
start_time = timer()

# Read tweets and grid boxes from JSON files
tweets_coordinates = readJSONTweetFile(TwitterFilePath)  
print("# of tweets read:", tweets_read)
print("# of Coordinates that passed regex:", len(tweets_coordinates))
grid_boxes = readJSONGridboxFile(gridFilePath)

##--Parallelise--
# Categorize tweets into boxes
gridifyTweets(tweets_coordinates, grid_boxes)
print("Total tweets categorised:", tweets_categorised)
##---------------

# count total in rows and columns
r_counts = {'A':0 , 'B':0, 'C':0, 'D':0}
c_counts = {'1':0 , '2':0, '3':0, '4':0, '5':0}
tallyTweetsbyRowCol(r_counts, c_counts)
    
# Sort & display zones by counts, rows, columns
sorted_zones = sorted(box_counts.items(), key=operator.itemgetter(1), reverse=True)
sorted_rows = sorted(r_counts.items(), key=operator.itemgetter(1), reverse=True)
sorted_cols = sorted(c_counts.items(), key=operator.itemgetter(1), reverse=True)
printStats(sorted_cols, sorted_rows, sorted_zones)

# Print processing time
end_time = timer()
print("Duration:", round(end_time-start_time, 4), "sec(s).")

    
# coord_patt = re.compile('\\"coordinates\\":\\[(\\d{1,3}\\.\\d+),(-\\d{1,3}\\.\\d+)]') # matches all coordinates
# Regex finer: \"coordinates\":\[(14[45]\.\d+),(-3[78]\.\d+)] - matches only coordinates within the grid_boxes
# Link: https://regex101.com/r/FMOngC/1

# Regex for grid_boxes: \"([A-Z]\d)\", \"xmin\": (\d{1,3}\.\d+), \"xmax\": (\d{1,3}\.\d+), \"ymin\": (-\d{1,3}\.\d+), \"ymax\": (-\d{1,3}\.\d+)
# https://regex101.com/r/mk0heZ/1