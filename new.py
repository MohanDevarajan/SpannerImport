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

def insert_data(request):
  import re
  import csv
  import codecs
  import argparse
  import logging
  from collections import OrderedDict
  from google.oauth2 import service_account
  from google.cloud import storage, spanner
  import numbers
  import base64
  instance_id = request.args.get('instance_id')
  database_id = request.args.get('database_id')
  bucket_name = request.args.get('bucket_name')
  table_id = request.args.get('table_id')
  batchsize = request.args.get('batchsize')
  data_file = request.args.get('data_file')
  format_file = request.args.get('format_file')
  spanner_client = spanner.Client()
  instance = spanner_client.instance(instance_id)
  database = instance.database(database_id)
  # Generate a unique local temporary file name to allow multiple invocations
  # of the tool from the same parent directory, and enable path to
  # multi-threaded loader in future
  local_file_name = '/tmp/temp1.txt'
  fmtfile = '/tmp/temp2.txt'
  # TODO (djrut): Add exception handling
  download_blob(bucket_name, data_file, local_file_name)
  download_blob(bucket_name, format_file, fmtfile)
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
