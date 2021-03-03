gcloud spanner instances create mohan-spanner-id  --config=regional-europe-west2  --description=mohan-spanner-id --nodes=1
gcloud spanner databases create mohan_database --instance=mohan-spanner-id --ddl='CREATE TABLE ImportTest (importid INT64 NOT NULL, strCol STRING(100) NOT NULL, byteCol BYTES(100) NO
T NULL, dateCol DATE NOT NULL, tsCol TIMESTAMP NOT NULL, intCol INT64 NOT NULL, floatCol FLOAT64 NOT NULL,) PRIMARY KEY (importid)'
