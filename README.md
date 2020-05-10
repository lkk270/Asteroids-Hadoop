##Apache Hadoop & Apache Impala Data Analysis of NYPD Arrest Data

Download dataset from this link as a .tsv file: https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/data

#Setup

Move downloaded dataset into DUMBO.

```bash
scp <file_name.tsv> <NetID>@dumbo.es.its.nyu.edu:/home/<NetID>
```

Then move into HDFS/

```bash
hadoop fs -put /home/<NetID>/<file_name.tsv> /user/<NetID>/ 
```

#Data Cleaing and Formatting

(Assuming the source files are are stored in their according folders on DUMBO -- if not: use)
```bash
scp <file_name.java> <NetID>@dumbo.es.its.nyu.edu:/home/<NetID>/<folder_name>
```

In DUMBO navigate to folder where the data cleaning java files are and then do:
```bash
export HADOOP_LIPATH=/opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib
javac -classpath $HADOOP_LIPATH/hadoop/*:$HADOOP_LIPATH/hadoop-0.20-mapreduce/*:$HADOOP_LIPATH/hadoop-hdfs/* *.java -Xdiags:verbose //compiles
jar cvf <driver_name.jar> *.class //creates jar file
hadoop jar <driver_name.jar> <driver_name> /user/<NetID>/<data_set.tsv> /user/<NetID>/<output_folder> //runs jar 
```

#Base Count with Map Reduce
Config argument is to specify which field you want the count to be done on -- it is required:

num:

0 = arrestDate
1 = year
2 = crime_1
3 = crime_2
4 = borough
5 = precinct
6 = jurisdictionCode
7 = age
8 = gender
9 = race

In DUMBO navigate to folder where the base map reduce java files are and then do:
```bash
export HADOOP_LIPATH=/opt/cloudera/parcels/CDH-5.15.0-1.cdh5.15.0.p0.21/lib
javac -classpath $HADOOP_LIPATH/hadoop/*:$HADOOP_LIPATH/hadoop-0.20-mapreduce/*:$HADOOP_LIPATH/hadoop-hdfs/* *.java -Xdiags:verbose //compiles
jar cvf BaseDriver.jar *.class  //creates jar file
hadoop jar <driver_name.jar> <driver_name> /user/<NetID>/<output_folder_name_from_data_cleaning_step>/part-r-00001 /user/<NetID>/<output_folder> -D config=<num>//runs jar 
```




