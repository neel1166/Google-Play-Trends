# Google-Play-Trends
Centered Analysis around Genres
Genre mapped against Installed
Better determine what attracts users for an initial installs
Genre mapped against Ratings
Once installed, which apps perform the best
Genre mapped against Free/Paid
Which apps are the most successful in generating revenue other than in-app advertisement.

1. Decompress the RAR file.
    unzip cs643_application_group1.zip

2. Staring Hadoop Cluster:
    hdfs namenode -format
    $HADOOP_HOME/sbin/start-dfs.sh
    jps  --> to check the proprer working of hadoop

3. Compile .java FILE
    hadoop com.sun.tools.javac.Main GooglePlay.java

4. Create Jar file
    jar cf googleplay.jar GooglePlay*.class

5. Copy .csv file to HDFS
    hadoop fs -copyFromLocal [path_to_.csvfile] /input/
    
6. Run Mapreduce job.
    hadoop jar googleplay.jar GooglePlay /input/ /output1/ /output2/ /output3/ /output4/ /output5/

7. Reading the output of file.
    hadoop fs -cat /output1/part-r-00000 --> Mapping Genres and type
    hadoop fs -cat /output2/part-r-00000 --> Mapping Genres and Rating
    hadoop fs -cat /output3/part-r-00000 --> Mapping Genre and No. of Installs
    hadoop fs -cat /output4/part-r-00000 --> Counting Genres
    hadoop fs -cat /output5/part-r-00000 --> Counting Apps (free and paid)


