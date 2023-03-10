import logging
from library_pipeline import createGTFS, joinAndexportGTFS, ExtractDrtTrips as edt, importCells, GTFSpreprocessing, \
    Legs2df, ExtractHubs, InterpolateTrips, SpacialOperations as sops

# Constants
cityName = 'Paris'
legs_input_path = cityName + '/output_legs.csv' # MATSim output legs csv file
studyarea_input_path = cityName + '/shp/cps.shp' # shapefile .shp for the DRT study area
popArea_input_path = cityName + '/shp/parisPopArea.shp'
gtfs_input_dir = cityName + '/gtfs/basecase/' # path to where gtfs.zip is stored

# EPSG code !!ONLY NUMBER!! EPSG:3035 --> 3035
crs_import = 2154 # crs of the raw imported data
crs_working = 2154 # crs used for the processing (can be same as crs_import)

# Toggle for preprocessing steps
# GTFS preprocessing to get hexagonal grid
#      --> needed once, can be skipped if GTFScells.csv file exists
toggle_GTFSprep = False
GTFScells_path = cityName + '/output/GTFScells.csv'

# Simulation & analysis start and end time
# Interpolation will only be done in this time window
start_time = '06:00:00'
end_time = '22:00:00'
# Length of each analysis interval
deltaT = 1 # in hours (h), default 1h



# NO MORE CHANGES REQUIRED BELOW THIS LINE

# Setup of the logging module
logging.basicConfig(filename= cityName + '/output/Plans2GTFS.log',
                    level=logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# Reading Matsim output_legs.csv file and converting it to pandas df
df = Legs2df.run(legs_input_path)

# converting trip coordinates [start, end] from simulation crs to new crs
edt.convertCRS(df,crs_import, crs_working,'start_x', 'start_y','end_x', 'end_y')

# Preparation for conversion to Geopandas dataframe
# Create Well-Known-Text (WKT) format of origin and destination
edt.constructWktPoint(df, 'start_x', 'start_y', 'origin')
edt.constructWktPoint(df, 'end_x', 'end_y', 'destination')
# Create Geopandas dataframe with geometry origin
df = sops.createGeoDataFrame(df['origin'], 'EPSG:' + str(crs_working), df)
# Ensure consistency of the crs
df = df.to_crs(crs=crs_working)

# Detecting hubs from trip data
DfHubs = ExtractHubs.run(df)
# Assigning each trip to the found hubs
ExtractHubs.assignHub2Trip(DfHubs, df)

# Reading gtfs file and getting hexagonal grid within bounding box of gtfs
if toggle_GTFSprep: GTFSpreprocessing.run(GTFScells_path)
# Importing the created cells
CellsDF = importCells.run(GTFScells_path, crs_working)
CellsDF.to_csv(cityName + '/output/cellsRaw.csv',sep=';')

# only keep cells that are within the respected study area given in the study_area shapefile
CellsDF = sops.removeExcessGrid(CellsDF, studyarea_input_path, crs_working, popArea_input_path, False)

CellsDF = sops.countTripsPerCell(CellsDF, df)
sops.addCellId2Trip(CellsDF,df)

# interpolating trip attributes from trips to centroids of cells
CellsDF = InterpolateTrips.runAllIntervals(df, CellsDF, deltaT, [start_time, end_time], DfHubs)

# creating a gtfs file from statistical interpolated trips from each zone to hub
drtGTFS = createGTFS.run(CellsDF, DfHubs, deltaT, [start_time, end_time], crs_working)

# read gtfs.zip file given at the path, combines it with drtGTFS and outputs it as gtfsOUT at the given path
joinAndexportGTFS.run(gtfs_input_dir, drtGTFS)


# ALL LINES BELOW ONLY CREATE FILE FOR ANALYSIS PURPOSES

df.to_csv(cityName + 'output/trips.csv',sep=';')
DfHubs.to_csv(cityName + 'output/hubs.csv',sep=';')
CellsDF.to_csv(cityName + 'output/cells.csv',sep=';')






