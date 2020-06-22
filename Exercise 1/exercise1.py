from pyspark import SparkContext
sc = SparkContext(appName="radioListeners")
sc.setLogLevel("WARN")

import sys
import os
from operator import add, itemgetter

def split_file_num(line):
  (title,numListeners) = line.split(',')
  numListeners = int(numListeners)
  return (title,numListeners)

def split_file_cad(line):
  (title,radioStation) = line.split(',')
  return (title, radioStation)

# The number of input arguments is checked and the first argument is read
if len(sys.argv) != 2:
  print("It requires an input argument representing the radio station. Aborting...")
  sys.exit(0)

radioStation=sys.argv[1]

file = open("output.txt", "w")
print(("Getting audience for themes aired on "+ radioStation), file = file)
file.close()

#Obtain the titles emitted by the radio station indicated by argument
titles_radioStation_files = sc.textFile("file_cad*.txt").\
                            map(split_file_cad).\
                              filter(lambda keyValue: keyValue[1]==radioStation).\
                                keys().\
                                  collect()

#Broadcast the lookup dictionary to the cluster
titles_radioStation_files_lookup = sc.broadcast(titles_radioStation_files)

#Obtain the total number of listeners to the titles emitted by the indicated radio station
titles_numListeners_files = sc.textFile("file_num*.txt").\
                              map(split_file_num).\
                                filter(lambda keyValue: keyValue[0] in titles_radioStation_files_lookup.value).\
                                  reduceByKey(add).\
                                    collect()

#Sort the output by titles
output = sorted(titles_numListeners_files)

#Save the output
file = open("output.txt", "a")
for o in output:
  print("%s: %d" % (o[0], o[1]), file=file)
file.close()
