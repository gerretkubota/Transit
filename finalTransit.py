# WORKING VERSION OF THE TRANSIT PROGRAM
# ADJUST THE
# importing libs
import sys
import csv
import os
import collections
import pandas as pd

from itertools import izip
#from pandas import *

# Global variables #
fileOne = ""
fileTwo = ""
service = ""
newFileA = ""
newFileB = ""
finalDirectFile = ""
finalIndirectFile = ""
prefinalFile = ""
segmentType = ""
# empty dictionaries
actual_arrival1 = {}
actual_arrival2 = {}
tempDict = {}
valueHolder = {}
# empty temp list holder
tempList1 = []
tempList2 = []
# row tracker array that holds a place for null value in a row
rowTrackerArray = [0]
# row tracker
rowTracker = 0
tracker = 0
filesProduced = 0

prevTempKeyName = ""
prevTimeGap = ""
prevDate = ""

dirInput = ""

# function for reading calculating actual arrival
def actualArrivalCalc(files, actual_arrival):
    global tempList1
    global tempList2
    global rowTracker
    global rowTrackerArray
    global prevTempKeyName
    global prevTimeGap
    global prevDate

    with open(files, 'rb') as csvinput:
        input_file = csv.DictReader(csvinput)
        # store the csv values into a global tempList array
        tempList1 = list(input_file)
        totalrows = len(tempList1)
        for row in tempList1:
            # ignore blank rows
            if any(val in (None, "") for val in row.itervalues()):
                break
            # if the row is not blank
            else:
                # row tracker
                rowTracker += 1
                # store value of arrival_time, departure_delay, and serviceID in variables
                arriveTime = row['arrival_time']
                departDelay = row['departure_delay']
                serviceID = row['service_id']
                # check if there is any value in both arriveTime and departDelay
                if arriveTime is None or departDelay is None or serviceID is None:
                    if rowTrackerArray[0] == 0:
                        rowTrackerArray[0] = rowTracker
                    # keep track where data is missing
                    rowTrackerArray.append(rowTracker)
                    continue
                else:
                    # split the timestamp string
                    # this will split into 3 string values of date, time, and gmt into an array
                    tempString = row['timestamp'].split(' ')
                    tempDate = tempString[0]
                    # hold string time split because it has a .
                    tempTime = tempString[1].split('.')
                    # convert the HH:MM:SS time to seconds and parse it as an int
                    tempSec = int(getSec(tempTime[0]))
                    # same conversion here
                    arriveTime = int(getSec(arriveTime))
                    # current row gap calculation
                    tempGapCalc = arriveTime - tempSec
                    #
                    departDelay = int(departDelay)#int(getSec(departDelay)) ## COME BACK TO THIS!!@#!@#!@#!@#@!#@!#!@@!#
                    tempTotal = arriveTime + departDelay

                    tempKeyName = row['timestamp'] + ' ' + row['trip_id'] + ' ' + row['service_id']
                    # after calculation of arriveTIme + departDelay -> convert sec to time in string
                    tempConverted = getTime(tempTotal)
                    # store in the key:value (concatenated string of date/tripid/stopsequence : actual arrival data)
                    checkClosest(actual_arrival, tempKeyName, tempDate, tempGapCalc, tempConverted)
                # reset tracker
                rowTracker = 0
        # clear contents inside list
        #print(actual_arrival)
        del tempList1[:]
        del tempList2[:]
        prevTempKeyName = ""
        prevTimeGap = ""
        prevDate = ""
        return;
# combine calculations and determine which one is a and b in this function?
# pass in the newly made temp files
def finalCalculations(file1, file2, file3):
    #determine which one is A and B to subtract appropriately (B-A)
    # do that by comparing stop_squence value
    # where (a < b) -> a = start, b = end
    # result should be in minutes
    global finalDirectFile
    global finalIndirectFile
    global tempList1
    global tempList2
    global segmentType
    global actual_arrival1
    global actual_arrival2
    global tracker
    global filesProduced
    # open up the two finalized direct and indirect files and open the final file to write on
    with open(file1, 'rb') as inputA, open(file2, 'rb') as inputB:
        with open(file3, 'ab') as csvWriter:
            writer = csv.writer(csvWriter, dialect='excel')
            # convert the files being read as a DictReader (hashmap)
            input_fileA = csv.DictReader(inputA)
            input_fileB = csv.DictReader(inputB)
            # convert the DictReader as a list (array)
            tempList1 = list(input_fileA)
            tempList2 = list(input_fileB)
            # initialize temp variables
            tempKeyNameA = ""
            tempKeyNameB = ""
            timeStringA = ""
            timeStringB = ""
            tempDateA = ""
            timeDateB = ""
            tempNameA = ""
            tempNameB = ""
            # nested loop, the whole purpose is to make a temp key in each row
            # and check to see if it's in the actual_arrival dictionaries made prior
            # which determines the NEEDED and NECESSARY values for the final output
            for rowA in tempList1:
                # split the timestamp
                tempStringA = rowA['timestamp'].split(' ')
                tempDateA = tempStringA[0]
                # initialize the tempKeyName
                tempKeyNameA = rowA['timestamp'] + ' ' + rowA['trip_id'] + ' ' + rowA['service_id']
                tempNameA = tempDateA+rowA['trip_id']+rowA['service_id']
                # if the tempKeyName exists in the actual_arrival dictionary, go to the next loop
                if tempKeyNameA in actual_arrival1:
                    for rowB in tempList2:
                        # same process as the first for loop
                        tempStringB = rowB['timestamp'].split(' ')
                        tempDateB = tempStringB[0]
                        tempKeyNameB = rowB['timestamp'] + ' ' + rowB['trip_id'] + ' ' + rowB['service_id']
                        tempNameB = tempDateB+rowB['trip_id']+rowB['service_id']
                        if tempKeyNameB in actual_arrival2:
                            # check if the tempKeys match, to see if the
                            if tempNameA == tempNameB:
                                if int(rowA['stop_sequence']) < int(rowB['stop_sequence']):
                                    tracker += 1
                                    arrivalA = actual_arrival1.get(tempKeyNameA)
                                    arrivalB = actual_arrival2.get(tempKeyNameB)

                            # value of actual run time, assign to actual_runtime
                                    runTime = getSec(arrivalB) - getSec(arrivalA)
                            # value of actual shape distance, assign to actual_distance
                                    shapeDist = float(rowB['shape_dist_traveled']) - float(rowA['shape_dist_traveled'])
                                    writer.writerow([segmentType, tempDateA, rowA['trip_id'], int(rowA['route_short_name']), rowA['stop_sequence'],
                                    rowA['stop_code'], rowA['arrival_time'], rowA['service_id'], rowA['stop_name'], rowA['shape_dist_traveled'],
                                    rowA['departure_delay'], arrivalA, rowB['stop_sequence'], rowB['stop_code'], rowB['arrival_time'],
                                    rowB['stop_name'], rowB['shape_dist_traveled'], rowB['departure_delay'], arrivalB, float(getMin(runTime)), shapeDist])
                                else:
                                    arrivalA = actual_arrival2.get(tempKeyNameB)
                                    arrivalB = actual_arrival1.get(tempKeyNameA)
                            # value of actual run time, assign to actual_runtime
                                    runTime = getSec(arrivalA) - getSec(arrivalB)
                            # value of actual shape distance, assign to actual_distance
                                    shapeDist = float(rowA['shape_dist_traveled']) - float(rowB['shape_dist_traveled'])
                                    writer.writerow([segmentType, tempDateA, rowB['trip_id'], int(rowB['route_short_name']), rowB['stop_sequence'],
                                    rowB['stop_code'], rowB['arrival_time'], rowB['service_id'], rowB['stop_name'], rowB['shape_dist_traveled'],
                                    rowB['departure_delay'], arrivalA, rowA['stop_sequence'], rowA['stop_code'], rowA['arrival_time'],
                                    rowA['stop_name'], rowA['shape_dist_traveled'], rowA['departure_delay'], arrivalB, float(getMin(runTime)), shapeDist])
                            else:
                                continue
                        else:
                            continue
                else:
                    continue

    if filesProduced == 1:
        try:
            fileArray = []
            fileArray.append(finalDirectFile)
            fileArray.append(finalIndirectFile)
            removeDups(fileArray)
            print(finalDirectFile)
            print(finalIndirectFile)
            header_saved = False
            with open(finalFile, 'wb') as csvWriter:
                for fileName in fileArray:
                    with open(fileName, 'rb') as csvReader:
                        header = next(csvReader)
                        if not header_saved:
                            csvWriter.write(header)
                            header_saved = True
                        for line in csvReader:
                            csvWriter.write(line)
            # remove the finaldirect and finalindirect file
            os.remove(finalDirectFile)
            os.remove(finalIndirectFile)
        except StopIteration:
            pass
        except OSError:
            pass
    filesProduced += 1
    return

# function that makes a new file
def makeTempFile(file, newFile):
    global rowTracker
    with open(file, 'rb') as csvRead:
        reader = csv.reader(csvRead)
        with open(newFile, 'wb') as csvWrite:
            writer = csv.writer(csvWrite)
            headers = reader.next()
            headers.append('actual_arrival')
            writer.writerow(headers)
            tempNum = 0
            for row in reader:
                if row == ['','','','','','','','','','','']:
                    continue
                elif rowTrackerArray[0] != 0 and rowTrackerArray[tempNum] == rowTracker:
                    tempNum += 1
                    continue
                else:
                    #writer.writerow(headers)
                    row.append('None')
                    writer.writerow(row)
                rowTracker += 1
            rowTracker = 0
    with open(newFile, 'rb') as readThis:
        read = csv.reader(readThis)
    return

# function that makes a new file
def makeFinalFile(newFile):
    with open(newFile, 'wb') as csvWrite:
        writer = csv.writer(csvWrite)
        writer.writerow(['segment_type', 'date', 'trip_id', 'route_short_name', 'stop_sequence-A', 'stop_code-A', 'arrival_time-A',
        'service_id', 'stop_name-A', 'shape_dist_traveled-A', 'departure_delay-A', 'actual_arrival-A', 'stop_sequence-B',
        'stop_code-B', 'arrival_time-B', 'stop_name-B', 'shape_dist_traveled-B', 'departure_delay-B', 'actual_arrival-B',
        'actual_runtime', 'actual_distance'])
    return

# function for converting seconds to time HH::MM:SS (returns as a string)
def getTime(s):
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return str(('%02d:%02d:%02d') % (h, m, s))

# function for converting seconds to MM:SS
def getMin(s):
    return (s // 60)

# function for converting time to seconds
def getSec(time):
    #print('time shit: ' + time)
    l = map(int, time.split(':'))
    return sum(n * sec for n, sec in zip(l[::-1], (1, 60, 3600)))

# function to check which value holds the closest between timestamp and arrival time
def checkClosest(actual_arrival, tempKeyName, tempDate, tempGapCalc, tempConverted):
    global prevTimeGap
    global prevTempKeyName
    global prevDate

    # check if the row is in a new date segment
    if tempDate != prevDate:
        actual_arrival[tempKeyName] = tempConverted
        prevDate = tempDate
        prevTimeGap = tempGapCalc
        prevTempKeyName = tempKeyName
    else:
        if tempGapCalc < prevTimeGap:
            del actual_arrival[prevTempKeyName]
            actual_arrival[tempKeyName] = tempConverted
            prevTempKeyName = tempKeyName
            prevTimeGap = tempGapCalc
    return

# function to remove dups
def removeDups(fileArray):
    for files in fileArray:
        rows = csv.reader(open(files, 'rb'))
        newrows = []
        for row in rows:
            if row not in newrows:
                newrows.append(row)
        writer = csv.writer(open(files, 'wb'))
        writer.writerows(newrows)
    return

# function that take in user input for two files and save it into global variables
def optionFunc(dirInput):
    global fileOne
    global fileTwo
    global newFileA
    global newFileB
    global finalDirectFile
    global finalIndirectFile
    global prefinalFile
    global finalFile
    global actual_arrival1
    global actual_arrival2
    global segmentType

    segmentType = raw_input("Is the data a direct or indirect service? ")

    newFileA = dirInput + 'newcsva.csv'
    newFileB = dirInput + 'newcsvb.csv'
    finalDirectFile = dirInput + 'finalcsvDirect.csv'
    finalIndirectFile = dirInput + 'finalcsvIndirect.csv'
    finalFile = dirInput + 'finalcsvFile.csv'

    if segmentType.lower() == 'direct':
        prefinalFile = finalDirectFile
        fileOne = raw_input("Enter file A name> ")
        fileOne = dirInput + 'Direct/' + fileOne
        fileTwo = raw_input("Enter file B name> ")
        fileTwo = dirInput + 'Direct/' + fileTwo
    elif segmentType.lower() == 'indirect':
        prefinalFile = finalIndirectFile
        fileOne = raw_input("Enter file A name> ")
        fileOne = dirInput + 'Indirect/' + fileOne
        fileTwo = raw_input("Enter file B name> ")
        fileTwo = dirInput + 'Indirect/' + fileTwo


    actualArrivalCalc(fileOne, actual_arrival1)
    actualArrivalCalc(fileTwo, actual_arrival2)
    # make new file with added column and if there's any
    # missing data, it should be skipped
    makeTempFile(fileOne, newFileA)
    makeTempFile(fileTwo, newFileB)
    makeFinalFile(prefinalFile)
    makeFinalFile(finalFile)
    finalCalculations(newFileA, newFileB, prefinalFile)
    return

# Ask for option1 or option2
while True:
    try:
        dirInput = raw_input("Enter the directory of the direct and indirect folder> ")
        if dirInput == 'exit':
            break
        else:
            optionFunc(dirInput)
            optionFunc(dirInput)

            os.remove(finalDirectFile)
            os.remove(finalIndirectFile)
            os.remove(newFileA)
            os.remove(newFileB)
            break
    except OSError:
        pass
