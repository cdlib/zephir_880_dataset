import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import time
import pymarc
from pymarc.exceptions import MissingLinkedFields

# Load environment variables
load_dotenv()

# get the title and 880 field from the marc json
def get_title_from_marc(marc):
    # Parse the MARC json data
    record = pymarc.parse_json_to_array(marc)[0]

    # Get the 245 field
    field245 = record['245']

    # Check if the 245 field exists
    if field245:
        title = field245.format_field()
    else:
        return None, None  # Return None if there is no 245 field

    # Get the linked 880 field associated with the 245 field
    try:
        linked_880s = record.get_linked_fields(field245)
    except MissingLinkedFields as e:
        print(f"An error occurred: {e}")
        linked_880s = None

    # If there is a linked 880 field, return the title and the formatted 880 field
    if linked_880s:
        return title, linked_880s[0].format_field()
    
    return title, None

# save the start time
start_time = time.time()
print("starting time: ", start_time)

# load the csv file into pandas
print("loading data")
load_time = time.time()
df = pd.read_csv('880_volumes_dataset.csv')
print("loading time: ", time.time()-load_time)

# drop duplicates using cid and contribsys_id
print("dropping duplicates")
drop_time = time.time()
df = df.drop_duplicates(subset=['cid', 'contribsys_id'], keep='first')
print("drop time: ", time.time()-drop_time)

# add expected selection order for zephir exports
print("adding selection order")
select_time = time.time()

selection_order = 1
current_cid = None
for index, row in df.iterrows():
    if current_cid != row['cid']:
        selection_order = 1
        current_cid = row['cid']
    df.at[index, 'selection_order'] = selection_order
    selection_order += 1


df['selection_order'] = df['selection_order'].astype(int)

# drop columns for selection order calculation
df.drop(columns=['var_usfeddoc', 'var_score', 'vufind_sort'], inplace=True)
print("selection order time: ", time.time() - select_time)

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


# Template for your SQL query
query = "SELECT id as htid, metadata_json FROM zephir_filedata WHERE id IN ({});"


# Chunk requests to the database for efficiency
id_list = df['htid'].to_list()
chunk_size = 10000
chunks = [id_list[x:x+chunk_size] for x in range(0, len(id_list), chunk_size)]
num_chunks = len(chunks)

print("starting query")
start_query_time = time.time()
# Iterate over the chunks with index
for i, chunk in enumerate(chunks):
    # Format the chunk into a comma-separated list of quoted IDs
    comma_id_list_chunk = ','.join(["'{}'".format(i) for i in chunk])
    formatted_query = query.format(comma_id_list_chunk)

    print("Querying chunk {} of {}".format(i+1, num_chunks))
    query_time = time.time()
    # Execute the query
    with mydb.cursor() as mycursor:
        mycursor.execute(formatted_query)
        results = mycursor.fetchall()

        for row in results:
            htid, metadata_json = row

            title, field880 = get_title_from_marc(metadata_json)

            # Add the title and 880 field to the dataframe
            df.loc[df['htid'] == htid, 'title'] = title
            df.loc[df['htid'] == htid, '880'] = field880
    print("query finished: ", time.time() - query_time)
# Close the database connection
mydb.close()
print("total query time for all records: ", time.time()-start_query_time)

# write the dataframe to a tsv file
print("writing file")
write_time = time.time()
df.to_csv('880_record_dataset.tsv', sep='\t', index=False)
print("write time: ", time.time()-write_time)

# print the time taken to run the script
print("total time: ", time.time()-start_time)