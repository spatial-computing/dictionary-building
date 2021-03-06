from OsmLoader import OsmLoader
from ShapefileLoader import ShapefileLoader
from GeoNamesLoader import GeoNamesLoader
from CsvLoader import CsvLoader
import os


# This is just an example.
# Fill in values based on your set up.

database='dictionary'
user='postgres'
password='postgres'

loader = OsmLoader('osm' + os.sep + 'great-britain-latest.osm.bz2')
# this is for testing
# loader = OsmLoader('osm' + os.sep + 'slough_england.osm')
loader.load_into_postgis(database, user, password)

loader = ShapefileLoader('ordnance-survey')
# this is for testing
# loader = ShapefileLoader('ordnance-survey' + os.sep + 'rndabout_point.shp')
loader.load_into_postgis(database, user, password)

loader = GeoNamesLoader('geonames' + os.sep + 'GB.txt')
loader.load_into_postgis(database, user, password)

loader = CsvLoader('csv' + os.sep + 'Potential_Contaminants_Classification.csv')
loader.load_into_postgis(database, user, password)