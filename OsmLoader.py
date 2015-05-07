from lxml import etree
import bz2
import sys
import json
import time
import os
import sqlite3
import psycopg2


class OsmLoader:
	def __init__(self, osm_file):
		self.osm_file = osm_file
		self.sqlite3_db = os.path.join(os.path.dirname(osm_file), 'sqlite3.db')


	def __load_into_sqlite(self):
		if os.path.exists(self.sqlite3_db):
			print('Remove ' + self.sqlite3_db)
			os.remove(self.sqlite3_db)

		osm_file = self.osm_file
		self.print_with_time('Start loading ' + osm_file + ' into ' + self.sqlite3_db)

		if osm_file.endswith('.bz2'):
			fin = bz2.BZ2File(osm_file, 'r')
		else:
			fin = open(osm_file, 'rb')

		conn = sqlite3.connect(self.sqlite3_db)
		cursor = conn.cursor()

		# Create table
		cursor.execute('''
			CREATE TABLE node (
				id TEXT PRIMARY KEY NOT NULL,
				coords TEXT
			)
		''')
		cursor.execute('''
			CREATE TABLE way (
				id TEXT PRIMARY KEY NOT NULL,
				coords TEXT
			)
		''')
		cursor.execute('''
			CREATE TABLE relation (
				id TEXT PRIMARY KEY NOT NULL,
				coords TEXT
			)
		''')
		conn.commit()

		context = etree.iterparse(fin, events=('end',), tag=('node', 'way', 'relation'))

		for event, elem in context:
			coords = []
			id = elem.attrib['id']
			type = elem.tag

			for child in elem:
				tag = child.tag

				if tag in ('nd', 'member'):
					cid = child.attrib['ref']
					if tag == 'nd':
						ctype = 'node'
					else:
						ctype = child.attrib['type']

					rows = cursor.execute('SELECT coords FROM ' + ctype + ' WHERE id=?', (cid,))
					for row in rows:
						coords.extend(json.loads(row[0]))

			if type == 'node':
				coords.append({
					'lat': float(elem.attrib['lat']),
					'lon': float(elem.attrib['lon']),
				})

			if coords:
				cursor.execute('INSERT INTO ' + type + ' VALUES(?, ?)', (id, json.dumps(coords)))

			elem.clear()
			while elem.getprevious() is not None:
				del elem.getparent()[0]

		fin.close()
		cursor.close()
		conn.commit()
		conn.close()

		self.print_with_time("Loading completed.")


	def load_into_postgis(self, database, user, password):
		self.__load_into_sqlite()

		osm_file = self.osm_file
		self.print_with_time('Start loading ' + osm_file + ' into ' + database)

		if osm_file.endswith('.bz2'):
			fin = bz2.BZ2File(osm_file, 'r')
		else:
			fin = open(osm_file, 'rb')

		osm_conn = sqlite3.connect(self.sqlite3_db)
		conn = psycopg2.connect(database=database, user=user, password=password)
		cursor = conn.cursor()

		# Create table
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS entity (
				id bigserial PRIMARY KEY,
				name text NOT NULL,
				category text,
				subcategory text,
				source text NOT NULL,
				create_time timestamp with time zone NOT NULL
			)
		''')
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS location (
				id bigint NOT NULL,
				geom geometry(POINT, 4326) NOT NULL
			)
		''')
		conn.commit()

		context = etree.iterparse(fin, events=('end',), tag=('node', 'way', 'relation'))
		prev = None
		for event, elem in context:
			item = {}
			item['cat'] = ''
			item['subcat'] = ''
			item['create_time'] = elem.attrib['timestamp']
			id = elem.attrib['id']
			type = elem.tag

			for child in elem:
				tag = child.tag

				if tag == 'tag':
					key = child.attrib['k']
					val = child.attrib['v']

					if key == 'name':
						item['name'] = val
					elif key in ('place', 'amenity', 'landuse', 'leisure', 'sport', 'tourism', 'shop', 'vending', 'historic', 'man_made', 'religion', 'natural', 'highway', 'railway', 'waterway', 'aeroway', 'aerialway', 'power', 'boundary', 'barrier'):
						item['cat'] = key
						item['subcat'] = val

			elem.clear()
			while elem.getprevious() is not None:
				del elem.getparent()[0]

			if 'name' in item:
				if not (prev and prev['name'] == item['name'] and prev['cat'] == item['cat'] and prev['subcat'] == item['subcat']):
					cursor.execute(
						'INSERT INTO entity(name, category, subcategory, source, create_time) VALUES(%s, %s, %s, %s, %s) RETURNING id',
						(item['name'], item['cat'], item['subcat'], 'OpenStreetMap', item['create_time'])
					)
					inserted_id = cursor.fetchone()[0]

				prev = item

				rows = osm_conn.execute('SELECT coords FROM ' + type + ' WHERE id=?', (id,))
				for row in rows:
					coords = json.loads(row[0])
					for coord in coords:
						cursor.execute(
							'INSERT INTO location VALUES(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))',
							(inserted_id, coord['lon'], coord['lat'])
						)

		fin.close()
		cursor.close()
		conn.commit()
		conn.close()
		osm_conn.close()

		self.print_with_time("Loading completed.")


	def print_with_time(self, str):
		print(time.strftime("%H:%M:%S") + ' ' + str)


# test program
if __name__ == "__main__":
	osm_file = sys.argv[1]
	loader = OsmLoader(osm_file)

	loader.load_into_postgis(database='dictionary', user='postgres', password='postgres')
