#Hadoop HDFS Homework

Counting bids with the same ID. See bid.example.txt.

- mvn package
- hadoop jar target/homework-hdfs.jar hdfs://localhost:9000/input hdfs://localhost:9000/output.txt

also jvm option "-Dmultithreaded=N" could be used - if no N is specified, number of threads equals to number of CPU cores
