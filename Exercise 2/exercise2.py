from pyspark import SparkContext
sc = SparkContext(appName="radioListeners")
sc.setLogLevel("WARN")

import sys
import numpy as np
import matplotlib.pyplot as plt
from operator import add, itemgetter

def split_file_num(line):
  (title,numListeners) = line.split(',')
  numListeners = int(numListeners)
  return (title,numListeners)

def split_file_cad(line):
  (title,radioStation) = line.split(',')
  return (title,radioStation)

def plot(output):
  radioStation = [d[0] for d in output]
  numListeners  = [d[1] for d in output]

  y_pos = np.arange(len(radioStation))

  plt.bar(y_pos,numListeners)
  plt.xticks(y_pos, radioStation)

  plt.title('Total listeners per radio station')
  plt.show()

print("Getting the total listeners of each radio station")

#Obtain the total number of listeners for each title
titles_numListeners = sc.textFile("file_num*.txt").\
                        map(split_file_num).\
                          reduceByKey(add).\
                            collectAsMap()

#Broadcast the lookup dictionary to the cluster
titles_numListeners_lookup = sc.broadcast(titles_numListeners)

#Obtain the total number of listeners for each radio station
#   distinct-> Ensure there are no repeated associations of radio stations and titles
#   mapValues -> Replace the title of the song with the respective number of listeners
#   reduceByKey -> Sum up the listeners of each title to get the total number of listerners to the radio station
radioStation_numListeners = sc.textFile("file_cad*.txt").\
                              map(split_file_cad).\
                                distinct().\
                                  map(lambda x: (x[1],x[0])).\
                                    mapValues(lambda x: titles_numListeners_lookup.value[x]).\
                                      reduceByKey(add).\
                                        collect()

#Sort the output in decreasing order of audience
output = sorted(radioStation_numListeners, key=itemgetter(1))

for o in output:
  print("%s: %s" % (o[0], o[1]))

#Display the chart with the audience information of each radio station
plot(output)