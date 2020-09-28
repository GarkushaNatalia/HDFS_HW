****Project for reading *.dat file from HDFS and converting to *.avro and uploading back into HDFS****

Preconditions:
 - Java 1.8
 - HDFS running - get hdfs connection string, ex: ""hdfs://0.0.0.0:8020""
 - airlines.dat file located in /user/student/airlines/
 - avro scheme for the file located in /user/student/airlines/

 Execution:

  - mvn package
  - java -jar hdfs-hw-1.0-SNAPSHOT-jar-with-dependencies.jar
