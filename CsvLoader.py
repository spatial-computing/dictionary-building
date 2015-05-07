import sys
import time
import datetime
import psycopg2
import csv


class CsvLoader:
	def __init__(self, filename):
		self.filename = filename


	def load_into_postgis(self, database, user, password):
		conn = psycopg2.connect(database=database, user=user, password=password)

		# Create table
		cursor = conn.cursor()
		cursor.execute('''
			CREATE TABLE IF NOT EXISTS general_terms (
				name text NOT NULL,
				source text NOT NULL,
				create_time timestamp with time zone NOT NULL
			)
		''')

		filename = self.filename
		self.print_with_time('Start loading ' + filename + ' into ' + database)

		with open(filename, newline='') as csvfile:
			reader = csv.reader(csvfile)

			for line in reader:
				name = line[0] + ' ' + line[2]

				cursor.execute(
					'INSERT INTO general_terms(name, source, create_time) VALUES(%s, %s, %s)',
					(name, 'Potential Contaminated Area', datetime.datetime.now())
				)

		conn.commit()
		cursor.close()
		conn.close()

		self.print_with_time("Loading completed.")


	def print_with_time(self, str):
		print(time.strftime("%H:%M:%S") + ' ' + str)


# test program
if __name__ == "__main__":
	filename = sys.argv[1]
	loader = CsvLoader(filename)

	loader.load_into_postgis(database='dictionary', user='postgres', password='postgres')