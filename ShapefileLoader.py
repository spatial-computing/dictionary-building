import sys
import time
import datetime
import os
import psycopg2
import shapefile


class ShapefileLoader:
	def __init__(self, source):
		self.source = source


	def load_into_postgis(self, database, user, password):
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

		source = self.source
		self.print_with_time('Start loading ' + source + ' into ' + database)

		if os.path.isfile(source):
			self.__load_shapefile_into_postgis(source, conn, cursor)
		elif os.path.isdir(source):
			for root, dirs, files in os.walk(source):
				for name in files:
					self.__load_shapefile_into_postgis(os.path.join(root, name), conn, cursor)

		cursor.close()
		conn.commit()
		conn.close()

		self.print_with_time("Loading completed.")


	def __load_shapefile_into_postgis(self, filename, conn, cursor):
		if not filename.endswith('.shp'):
			return

		self.print_with_time('Start loading ' + filename)

		sf = shapefile.Reader(filename)
		indices_of_name_field = [i-1 for i, attr in enumerate(sf.fields) if 'NAME' == attr[0]]

		if not indices_of_name_field:
			indices_of_name_field = [i-1 for i, attr in enumerate(sf.fields) if 'NAME' in attr[0]]
		if not indices_of_name_field:
			return
		if len(indices_of_name_field) > 1:
			print('Warning: There are multiple fields containing "NAME" in file ', filename)

		category = os.path.basename(filename).rpartition('.')[0]
		shapeRecords = sf.iterShapeRecords()

		for shape, record in shapeRecords:
			name = ', '.join([record[i] for i in indices_of_name_field if isinstance(record[i], str)])
			if not name:
				continue

			cursor.execute(
				'INSERT INTO entity(name, category, source, create_time) VALUES(%s, %s, %s, %s) RETURNING id',
				(name, category, 'Ordnance Survey', datetime.datetime.now())
			)

			inserted_id = cursor.fetchone()[0]

			for point in shape.points:
				cursor.execute(
					'INSERT INTO location VALUES(%s, ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 27700), 4326))',
					(inserted_id, point[0], point[1])
				)

			# self.print_with_time('Commit ' + str(inserted_id))
		conn.commit()


	def print_with_time(self, str):
		print(time.strftime("%H:%M:%S") + ' ' + str)


if __name__ == "__main__":
	source = sys.argv[1]
	loader = ShapefileLoader(source)

	loader.load_into_postgis(database='dictionary', user='postgres', password='postgres')