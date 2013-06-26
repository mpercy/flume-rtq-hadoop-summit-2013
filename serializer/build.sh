#!/bin/sh -xe
############################################
mvn clean package
mkdir -p target/stage/demo-csv-avro-serializer/lib
cp target/demo-csv-avro-serializer-1.0-SNAPSHOT.jar target/stage/demo-csv-avro-serializer/lib
cd target/stage/
tar czvf demo-csv-avro-serializer.tar.gz demo-csv-avro-serializer
echo "DONE: target/stage/demo-csv-avro-serializer.tar.gz"
