# Starting on Apache SPARK

These two exercises were made in the subject of "Computación Distribuída e de Altas Prestacións" in the Master Degree of Computer Engineering of the University of Vigo in 2020

### Exercise 1

A series of files are provided containing audience data for topics broadcast on radio stations:
- The file_cad?.txt files consist of a list of music tracks and, for each track, the radio station on which it was broadcast.
- The files file_num?.txt also contain playlists and, for each track, the number of listeners it has had.

The objective of this exercise is to implement a python program for Apache Spark that provides an answer to the following question:

*Given a radio station, what has been the total number of listeners (on all radio station) to the topics that have been broadcast by that network?*

The program will be execute with the following commands:

` $ export PYSPARK_PYTHON=python3`

` $ spark-submit ./exercise1.py RockFM`

### Exercise 2

A program will be made that will compare the audience of themes broadcasted for the different radio stations. To this end:

1. You will get the list of radio stations.
2. For each station, you will get the total number of listeners (in all stations) of the subjects broadcast by that station.
3. It shall display a list of the stations and total listeners to the subjects issued by them in decreasing order of hearing.
4. Using the matplotlib library, you will create a graph with the same information.