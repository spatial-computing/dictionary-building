import sys
import time
import datetime
import psycopg2


class GeoNamesLoader:
	def __init__(self, filename):
		self.filename = filename


	def load_into_postgis(self, database, user, password):
		conn = psycopg2.connect(database=database, user=user, password=password)

		# Create table
		cursor = conn.cursor()
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

		filename = self.filename
		self.print_with_time('Start loading ' + filename + ' into postgis ' + database)

		with open(filename) as f:
			for line in f:
				values = line.split('\t')
				name = values[1]
				lat = float(values[4])
				lon = float(values[5])
				cat = values[6]
				subcat = values[7]

				cursor.execute(
					'INSERT INTO entity(name, category, subcategory, source, create_time) VALUES(%s, %s, %s, %s, %s) RETURNING id',
					(name, cat, subcat, 'GeoNames', datetime.datetime.now())
				)

				inserted_id = cursor.fetchone()[0]

				cursor.execute(
					'INSERT INTO location VALUES(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))',
					(inserted_id, lon, lat)
				)

		conn.commit()
		cursor.close()
		conn.close()

		self.print_with_time("Loading completed.")


	def print_with_time(self, str):
		print(time.strftime("%H:%M:%S") + ' ' + str)


if __name__ == "__main__":
	filename = sys.argv[1]
	loader = GeoNamesLoader(filename)

	loader.load_into_postgis(database='dictionary', user='postgres', password='postgres')