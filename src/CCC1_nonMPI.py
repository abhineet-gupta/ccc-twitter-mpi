'''
@author: Abhineet Gupta
'''

import re   #regex library
from timeit import default_timer as timer

if __name__ == '__main__':    
    startTime = timer()
    
    TwitterFilePath = 'tinyTwitter.json'
    gridFilePath = 'MelbGrid.json'
    
    # Store tweets_coordinates found in file
    tweets_coordinates = []    # list of (longitude, latitude) tuples
    grid_boxes = []           # list of (ID, xmin, xmax, ymin, ymax) tuples
    box_counts = {}
    
    # Regex for matching tweets_coordinates in JSON file for each tweet
    coord_patt = re.compile('\\"coordinates\\":\\[(14[45]\\.\\d+),(-3[78]\\.\\d+)]')
    
#     Read file line by line
    with open(TwitterFilePath, 'r', encoding='ansi') as fin:
        for line in fin:    
            temp_res = coord_patt.findall(line)
            
            if (temp_res): # if results are not null
    #             Append tweets_coordinates found into list, as a tuple of floats       
                tweets_coordinates.append((float(temp_res[0][0]), float(temp_res[0][1])))
    
    print("Number of lines read:\n", len(tweets_coordinates))
    print("First and last elements:\n", tweets_coordinates[0], tweets_coordinates[-1])

#     Read grid_boxes
    grid_patt = re.compile('\\"([A-Z]\\d)\\", \\"xmin\\": (\\d{1,3}\\.\\d+), \\"xmax\\": (\\d{1,3}\\.\\d+), \\"ymin\\": (-\\d{1,3}\\.\\d+), \\"ymax\\": (-\\d{1,3}\\.\\d+)')
    
    with open(gridFilePath, 'r') as fin:
        for line in fin:
            temp_box = grid_patt.findall(line)
            if (temp_box):
                grid_boxes.append((temp_box[0][0], float(temp_box[0][1]), float(temp_box[0][2]), float(temp_box[0][3]), float(temp_box[0][4])))
                box_counts[temp_box[0][0]] = 0
    
    print("Number of grid_boxes boxes read:\n", len(grid_boxes))
    print("First and last elements:\n", grid_boxes[0], grid_boxes[-1])
    
    tweets_categorised = 0
    
#     Categorize tweets
    for tweet_loc in tweets_coordinates:
        for grid_box in grid_boxes:
            if tweet_loc[0] > grid_box[1] and tweet_loc[0] <= grid_box[2] and tweet_loc[1] >= grid_box[3] and tweet_loc[1] < grid_box[4]:
                box_counts[grid_box[0]]+=1
                tweets_categorised+=1
                  
    for grid_box in grid_boxes:
        print(grid_box[0], ":", box_counts[grid_box[0]])
    print("Total tweets categorised:", tweets_categorised)
    
#     Print processing time
    endTime = timer()
    print("Duration:", round(endTime-startTime, 4), "sec(s).")
    
    
# coord_patt = re.compile('\\"coordinates\\":\\[(\\d{1,3}\\.\\d+),(-\\d{1,3}\\.\\d+)]') # matches all coordinates
# Regex finer: \"coordinates\":\[(14[45]\.\d+),(-3[78]\.\d+)] - matches only coordinates within the grid_boxes
# Link: https://regex101.com/r/FMOngC/1

# Regex for grid_boxes: \"([A-Z]\d)\", \"xmin\": (\d{1,3}\.\d+), \"xmax\": (\d{1,3}\.\d+), \"ymin\": (-\d{1,3}\.\d+), \"ymax\": (-\d{1,3}\.\d+)
# https://regex101.com/r/mk0heZ/1