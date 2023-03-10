import sys
sys.path.insert(0, './library/')
import pymongo as pym
import geopandas as gpd
import geojson
import csv
import imp

# general info
city = 'Paris'
scenario_name = 'drt' # e.g. 'drt'

# file paths and addresses to be provided:
# population data
shpPath = city +'/shp/pop.shp'
popCollectionName = "POP"
popField = "pop"

# Study area
# Study area defines the area in which the accessibility calaculation is executes
study_area_shp_path = city + '/shp/studyarea.shp'


directoryGTFS = './' + city +'/gtfs/'  # !! only directory of gtfs.zip file, not the path to file
gtfs_prep_toggle = True
urlMongoDb = "mongodb://localhost:27017/"  # url of the mongodb database
urlMongoDbPop = "mongodb://localhost:27017/" # url of the mongodb database for population
urlServerOsrm = 'http://localhost:5000/' # url of the osrm server of the city

# required parameters
# regarded day from gtfs file
day = '20220307'
dayName = "monday"  # name of the corresponding day

# List of starting time for computing the isochrones
# Sync to operation hours of drt, pt and conversion timeframe
timeList = list(range(6, 23, 1)) # -->[7,8,9,10,11]
# timeList = [7,10,13,16,19,22]
hStart = timeList[0]*3600 # converting to seconds

# parameters of walking distance
timeWalk = 15 * 60  # seconds
velocityWalk = 1.39  # m/s ***5km/h***
distanceS = timeWalk * velocityWalk

# Parameters thst define the resolution and extention of tesselletion and the maximum of the walking time
# grid step of the hexagonal tesselletion in kilometers
gridEdge = 1

# Set check4stops = False if cells / hexagones should be included that do not have stops within.
# Set check4stops = False for preprocessing prior to dynamic mode to gtfs convertion
# Set check4stops = True for citychrone accessibility analysis
check4stops = True

# NO MORE CHANGES REQUIRED BELOW THIS LINE

client = pym.MongoClient(urlMongoDb)
gtfsDB = client[str('pta-' + city + '-' +scenario_name)]
popDbName = str('pta-' + city + '-' +scenario_name)

directoryGTFS = './'+ city + '/gtfs/'+ scenario_name +'/' # directory of the gtfs files.

# calculations

if gtfs_prep_toggle:
    from library.libConnections import printGtfsDate
    printGtfsDate(directoryGTFS)

    shapefile = gpd.read_file(shpPath)
    shapefile.to_file(shpPath.split('.')[0]+ '.geojson', driver='GeoJSON')
    with open(shpPath.split('.')[0]+ '.geojson') as f:
        gj = geojson.load(f)
    features = gj['features']
    gtfsDB["POP"].drop()
    gtfsDB["POP"].insert_many(features)

    from library.libStopsPoints import loadGtfsFile
    listOfFile = ['stops.txt', 'routes.txt', 'trips.txt', 'calendar.txt', 'calendar_dates.txt',
                  'stop_times.txt']  # , 'stop_times.txt']#, 'shapes.txt']
    loadGtfsFile(gtfsDB, directoryGTFS, city, listOfFile)

    from library.libConnections import readConnections
    readConnections(gtfsDB, city, directoryGTFS, day, dayName)

    from library.libStopsPoints import removingStopsNoConnections, setPosField, removeStopsOutBorder
    #removeStopsOutBorder(gtfsDB, city, 'OECD_city', ["commuting_zone", "city_core"])
    removingStopsNoConnections(gtfsDB, city)
    setPosField(gtfsDB, city)

    from library.libConnections import updateConnectionsStopName
    updateConnectionsStopName(gtfsDB, city)

from library.libStopsPoints import returnStopsList
stopsList = returnStopsList(gtfsDB, city)

if len(study_area_shp_path) != 0:
    print('Study area from Shapefile')
    from library.libStudyArea import extractBoundingBoxFromShp
    bbox = extractBoundingBoxFromShp(study_area_shp_path)
else:
    print('Study area from gtfs stop extend')
    from library.libStopsPoints import boundingBoxStops, mapStops
    bbox = boundingBoxStops(stopsList)
    mapStops(bbox, stopsList)

from library.libHex import hexagonalGrid
hexBin, pointBin = hexagonalGrid(bbox, gridEdge, gtfsDB['stops'], distanceS, city, True)


field_names = ["point","hex","city","served","pos"]

with open(city + '/output/names.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=';')
    writer.writeheader()
    writer.writerows(pointBin)

from library.libHex import insertPoints
insertPoints(pointBin, city, gtfsDB)

from library.libHex import pointsServed
pointsServed(gtfsDB, stopsList, urlServerOsrm, distanceS, timeWalk, city)

from library.libHex import settingHexsPos
settingHexsPos(gtfsDB, city)

from library.libHex import showHexs
showHexs(gtfsDB, city, 10)

from library.libHex import setHexsPop

if urlMongoDbPop != "" and popCollectionName != "":
    clientPop = pym.MongoClient(urlMongoDbPop)
    popDb = clientPop[popDbName]
    popCollection = popDb[popCollectionName]
    setHexsPop(gtfsDB, popCollection, popField, city)
else:
    print("Population NOT INSERTED!")

res = gtfsDB['points'].update_many({'pop':{'$exists':False}}, {'$set':{'pop':0}})
print("n° of matched hexagons with population Polygons: {0} \n not matched: {1} (setted to zero)".format(gtfsDB['points'].find({'pop':{'$exists':True}}).count(), res.modified_count))

from library.libStopsPoints import computeNeigh
computeNeigh(gtfsDB, urlServerOsrm, distanceS, timeWalk,  city)

from library.libConnections import makeArrayConnections
arrayCC = makeArrayConnections(gtfsDB, hStart, city)

from library.libStopsPoints import listPointsStopsN
arraySP = listPointsStopsN(gtfsDB, city)


import library.libAccessibility
imp.reload(library.libAccessibility)
from library.icsa import computeAccessibilities
imp.reload(library.icsa)

listAccessibility = ['velocityScore','socialityScore', 'velocityScoreGall',
                     'socialityScoreGall','velocityScore1h', 'socialityScore1h',
                    'timeVelocity', 'timeSociality']

computeIsochrone = False
if 'isochrones' in gtfsDB.collection_names():
    #gtfsDB['isochrones'].delete_many({'city':city})
    pass
for timeStart in timeList:
    timeStart *= 3600
    print('Time Isochrone Start: {0}'.format(timeStart/3600,))
    computeAccessibilities(city, timeStart, arrayCC, arraySP, gtfsDB, computeIsochrone, timeStart/3600 == timeList[0], listAccessibility=listAccessibility)

print('Computing averages:')
from library.libStopsPoints import computeAverage
computeAverage(listAccessibility, gtfsDB, city)

