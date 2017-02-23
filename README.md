#Hadoop First Try

basic setup to compile MapReduce job (word count) for Hadoop with Maven

To run download hadoop, create input folder with some text files and do:

- mvn package
- <path-to-hadoop>/bin/hadoop jar target/hadoop-test.jar input output