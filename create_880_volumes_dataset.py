
import csv
import os

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# use env variables for database connection
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_DATABASE = os.getenv("DB_DATABASE")

# connect to database
mydb = mysql.connector.connect(
  host
  =DB_HOST,
  user=DB_USERNAME,
  password=DB_PASSWORD,
  database=DB_DATABASE
)

query = """
SELECT cid, zr.namespace, zr.contribsys_id, zr.id as htid, zr.language, var_usfeddoc, var_score, concat(cid,'_',zr.autoid) as vufind_sort
FROM zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id
WHERE attr_ingest_date is not null
AND zf.metadata LIKE '%tag="880"%code="6">245-%'
order by cid, var_usfeddoc DESC, var_score DESC, vufind_sort ASC
"""
print("starting query")
# write the results to a csv file
with mydb.cursor() as mycursor:
  mycursor.execute(query)
  print("writing file")
  with open('880_volumes_dataset.csv', mode='w', newline='') as file:
      writer = csv.writer(file)
      writer.writerow([i[0] for i in mycursor.description]) # write headers
      for row in mycursor:
          writer.writerow(row)

# Close the database connection
mydb.close()