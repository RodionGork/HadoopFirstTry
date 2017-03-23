# Hadoop First Try

basic setup to compile MapReduce job (word count) for Hadoop with Maven

To run download hadoop, create input folder with some text files (optionally,
run a script harvesting few books from project gutenberg), build with maven and run:

- php -f harvest-gutenberg.php
- mvn package
- hadoop jar target/hadoop-test.jar input output
