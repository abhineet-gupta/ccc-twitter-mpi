#!/usr/bin/env python

'''
@author: Abhineet Gupta
CCC Assignment 1; Sem 1, 2017
Mapping Twitter Data using MPI on SPARTAN
'''

import numpy
import copy
from mpi4py import MPI
 
# Get MPI Info
comm = MPI.COMM_WORLD
proc_rank = comm.Get_rank()
proc_size = comm.Get_size()

import re   #regex library
from timeit import default_timer as timer
import operator

TwitterFilePath = 'smallTwitter.json'
gridFilePath = 'melbGrid.json'

# Regex for matching tweets_coordinates in JSON file for each tweet
coord_patt = re.compile('\\"coordinates\\":\\[(14[45]\\.\\d+),(-3[78]\\.\\d+)]')
# Regex for gridbox coordinates
grid_patt = re.compile('\\"([A-Z]\\d)\\", \\"xmin\\": (\\d{1,3}\\.\\d+), \\"xmax\\": (\\d{1,3}\\.\\d+), \\"ymin\\": (-\\d{1,3}\\.\\d+), \\"ymax\\": (-\\d{1,3}\\.\\d+)')

# list of (ID, xmin, xmax, ymin, ymax) tuples
grid_boxes = [] # all processes should fill it themselves exactly the same

# numpy list of (longitude, latitude) tuples
tw_coord_narray = None  # each process will get a subset via scatter

# number of coordinates read - each process gets this value broadcasted
num_coordinates = numpy.zeros(1)

# dict to store each zone and its tweet count
box_counts = {} # all processes initialise it to zero; but each calculates its own

# tweets that have been boxed
tweets_categorised = numpy.zeros(1) # each need to calculate its own

def readJSONTweetFile(filePath, tweets_read):    
    tweets_coordinates = []
    # Read file line by line
    with open(filePath, 'r') as fin:
        for line in fin:
            tweets_read[0] += 1    
            temp_res = coord_patt.findall(line)
            if (temp_res): # if results are not null
    #             Append coordinates found into list, as a tuple of floats       
                tweets_coordinates.append((float(temp_res[0][0]), float(temp_res[0][1])))
    return tweets_coordinates

def readJSONGridboxFile(filepath):
    global box_counts, grid_boxes
    
    with open(filepath, 'r') as fin:
        for line in fin:
            temp_box = grid_patt.findall(line)
            if (temp_box):
                grid_boxes.append((temp_box[0][0], float(temp_box[0][1]), float(temp_box[0][2]), float(temp_box[0][3]), float(temp_box[0][4])))
                box_counts[temp_box[0][0]] = numpy.zeros(1)

def gridifyTweets(tw_coord):
    global box_counts, tweets_categorised
    
    for tweet_loc in tw_coord:
        for grid_box in grid_boxes:
            if tweet_loc[0] > grid_box[1] and tweet_loc[0] <= grid_box[2] \
            and tweet_loc[1] >= grid_box[3] and tweet_loc[1] < grid_box[4]:
                box_counts[grid_box[0]][0] += 1
                tweets_categorised[0] += 1

def tallyTweetsbyRowCol(row_counts, col_counts, box_counts):
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
        print(zone[0] + ":", int(zone[1][0]), "tweets.")
    print("")            
    for row in sorted_rows:
        print(row[0] + "-Rows:", int(row[1]), "tweets.")
    print("")
    for col in sorted_cols:
        print("Column", col[0] + ":", int(col[1]), "tweets.")
    print("")
    
start_time = timer()

readJSONGridboxFile(gridFilePath)
final_box_counts = copy.deepcopy(box_counts)

if proc_rank == 0:

    # Read tweets and grid boxes from JSON files
        # num tweets read (minus the two lines for [ and ])
    tweets_read = [-1]
    tw_coord_narray = numpy.array(readJSONTweetFile(TwitterFilePath, tweets_read))
    print("# of tweets read:", tweets_read[0])
    num_coordinates[0] = len(tw_coord_narray)

    print("# of Coordinates passed regex:", num_coordinates[0]) 
    print("Num proc:", proc_size)
    print("Num coord:", num_coordinates[0])
    
    # send num of tweet_coords to each process (Bcast)
    # split the coords into proc_sized chunks (Scatter?)
    # send each slice off to the process
    # each proc gridifies their share
    # sends their box counts back
    # root node adds them up

    #--------------------------------------------------------------------------------------
    ##--Parallelise--
    # Categorize tweets into boxes
    
    # gridifyTweets(tw_coord_narray)
    # print("Total tweets categorised:", tweets_categorised)
    ##---------------

comm.Bcast(num_coordinates, root=0)

local_tweet_coord = numpy.zeros((int(num_coordinates[0]/proc_size), 2))
comm.Scatter(tw_coord_narray, local_tweet_coord, root=0)

gridifyTweets(local_tweet_coord)
print("Total tweets categorised:", tweets_categorised[0])

# if proc_rank == 1:
#     for grid_box in grid_boxes:
#         print (grid_box[0], box_counts[grid_box[0]], final_box_counts[grid_box[0]])


for grid_box in grid_boxes:
    comm.Reduce(box_counts[grid_box[0]], final_box_counts[grid_box[0]], op = MPI.SUM)

if proc_rank == 0:
    # count total in rows and columns
    r_counts = {'A':0 , 'B':0, 'C':0, 'D':0}
    c_counts = {'1':0 , '2':0, '3':0, '4':0, '5':0}
    tallyTweetsbyRowCol(r_counts, c_counts, final_box_counts)
        
    # Sort & display zones by counts, rows, columns
    sorted_zones = sorted(box_counts.items(), key=operator.itemgetter(1), reverse=True)
    sorted_rows = sorted(r_counts.items(), key=operator.itemgetter(1), reverse=True)
    sorted_cols = sorted(c_counts.items(), key=operator.itemgetter(1), reverse=True)
    printStats(sorted_cols, sorted_rows, sorted_zones)

    # Print processing time
    end_time = timer()
    print("Duration:", round(end_time-start_time, 4), "sec(s).")