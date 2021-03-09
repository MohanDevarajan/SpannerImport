import re
import csv
import gzip
import uuid
import codecs
import argparse
import logging
from collections import OrderedDict
from google.oauth2 import service_account
from google.cloud import storage, spanner
import numbers
import base64
import os

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
    
def isinteger(value):
	try:
		int(value)
		return True
	except ValueError:
		return False

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))

def insert_data(instance_id, database_id, bucket_name, table_id, batchsize, data_file, format_file):
  spanner_client = spanner.Client()
  instance = spanner_client.instance(instance_id)
  database = instance.database(database_id)
  # Generate a unique local temporary file name to allow multiple invocations
  # of the tool from the same parent directory, and enable path to
  # multi-threaded loader in future
  local_file_name = 'tmp'
  # TODO (djrut): Add exception handling
  download_blob(bucket_name, data_file, local_file_name)
  # Figure out the source and target column names based on the schema file
  # provided, and add a uuid if that option is enabled
  fmtfile = open(format_file, 'r')
  fmtreader = csv.reader(fmtfile)
  collist = []
  typelist = []
  icols = 0
  for col in fmtreader:
    collist.append(col[1])
    typelist.append(col[2])
    icols = icols + 1
  numcols = len(collist)
  ifile  = open(local_file_name, "r")
  reader = csv.reader(ifile,delimiter=',')
  next(reader)
  alist = []
  irows = 0
  for row in reader:
    for x in range(0,numcols):
      if typelist[x] == 'integer':
        row[x] = int(row[x])
      if typelist[x] == 'float':
        row[x] = float(row[x])
      if typelist[x] == 'bytes':
        row[x] = base64.b64encode(row[x])
    alist.append(row)
    irows = irows + 1
  ifile.close()
  rowpos = 0
  batchrows = int(batchsize)
  while rowpos < irows:
    with database.batch() as batch:
      batch.insert(
        table=table_id,
        columns=collist,
        values=alist[rowpos:rowpos+batchrows]
        )
    rowpos = rowpos + batchrows
  print('inserted {0} rows'.format(rowpos))
  os.remove('tmp')

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
    '--instance_id', help='Your Cloud Spanner instance ID.')
  parser.add_argument(
    '--database_id', help='Your Cloud Spanner database ID.',default='example_db')
  parser.add_argument(
    '--bucket_name', help='your bucket name')
  parser.add_argument(
    '--table_id', help='your table name')
  parser.add_argument(
	'--batchsize', help='the number of rows to insert in a batch')
  parser.add_argument(
    '--data_file', help='the csv input data file')
  parser.add_argument(
    '--format_file', help='the format file describing the input data file')
		
  args = parser.parse_args()
  insert_data(args.instance_id, args.database_id, args.bucket_name, args.table_id, args.batchsize, args.data_file, args.format_file)
