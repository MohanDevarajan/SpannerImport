def download_blob(bucket_name, data_file, format_file, local_file_name, local_file_format):
    from google.oauth2 import service_account
    from google.cloud import storage, spanner
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(data_file)
    blob_format = bucket.blob(format_file)
    blob.download_to_filename(local_file_name)
    blob_format.download_to_filename(local_file_format)
    print('Blob {} downloaded to {}.'.format(
        data_file,
        local_file_name))
    print('Blob {} downloaded to {}.'.format(
        format_file,
        local_file_format))
    with open(local_file_format,'r') as file:
        reader = file.read().split(",")
        print(reader)

def insert_data(request):
    from google.oauth2 import service_account
    from google.cloud import storage, spanner
    import csv 
    import os
    instance_id = request.args.get('instanceid')
    database_id = request.args.get('databaseid')
    bucket_name = request.args.get('bucketname')
    table_id = request.args.get('tableid')
    batchsize = request.args.get('batchsize')
    data_file = request.args.get('datafile')
    format_file = request.args.get('formatfile')
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    # Generate a unique local temporary file name to allow multiple invocations
    # of the tool from the same parent directory, and enable path to
    # multi-threaded loader in future
    local_file_name = '/tmp/temp.txt'
    local_file_format = '/tmp/format.txt'
    # TODO (djrut): Add exception handling
    download_blob(bucket_name, data_file, format_file, local_file_name, local_file_format)
    fmtfile = open(local_file_format, 'r')
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
