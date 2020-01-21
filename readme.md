# MapleJuice README

### Overall Process
1. Start up the SDFS nodes (at least 4 of them)
2. Start up master on VM01
3. Start up workers on all other VMs
4. Add files that will be needed by the maple/juice requests to the SDFS
5. Submit requests using maple/juice commandline utilities
6. [Optional] Start up the MP1 programs as stated in MP1 README for remote log grepping (the log for worker is named "worker_log.log", the log for master is named "mj_master.log". Master log and the worker log are stored in same working directory under which master and worker are run respectively.

### SDFS Nodes
#### Overall Process for Starting SDFS
1. Create 3 local directory named "log", "master" and "tmp"
2. Start up the server process (SDFS nodes) on VM01, join the server process (using the "introducer" command)
3. Start up and join all the other server processes on other VMs (using the "join" command). There should be at least a total of 4 SDFS nodes in the system.
4. The server is up and ready for file operations.
> Note that for the server on VM01 (introducer) to rejoin after leaving/failing, the server on VM02 must be alive and have joined the system

> Note that for the other server process to rejoin after leaving/failing, the server on VM01 (introducer) must be alive and have joined the system 

#### Compile and Run the Code
To run the SDFS node, use the following command:
```
python3 sdfs_node.py SDFS_MASTER_PORT SDFS_SLAVE_PORT SDFS_FD_PORT
```
For compatibility with MapleJuice master and worker, use 2000 for ```SDFS_MASTER_PORT```, use 2001 for ```SDFS_SLAVE_PORT``` and use 2002 for ```SDFS_FD_PORT``` 

The SDFS node will then provide a commandline interface where one can issue commands for the membership list components (including showing membership list, joining and leaving).

A list of available commands are shown as follows:
1. introducer: For the introducer of the SDFS to join the system, only used when starting up the SDFS. Note that when the introducer left the system and rejoins later, one should use the "join" command rather than the "introducer" command to rejoin.
2. join: For other SDFS nodes to join the system.
3. leave: For leaving the system.
4. membership list: For showing the current membership list.
5. membership list id: For showing the current membership list in IDs.
6. self id: For showing self ID.
7. get table: Shows the heartbeat sending and receiving table.
8. failure list: Deprecated, should not be used.
9. reverse list: Deprecated, should not be used.
10. exit: For exiting the program after leaving the system.

### MapleJuice Master
#### Overall Process for Starting Master
1. Start up the master specifying what kind of paritioning for juice tasks.
2. Start the master daemon using the commandline interface provided by the master.
3. The master is up and ready for requests.

> The master has to be run on VM01 by design

> Before starting the workers, the master has to be started first.

#### Compile and Run the Code
To run the MapleJuice master, use the following command:
```
python3 master.py PARTITIONING_TYPE
```
where ```PARTITIONING_TYPE``` is 0 for hash partitioning of juice request keys and 1 for range partitioning of juice request keys.

The master will then provide a commandline interface where one can issue commands for the starting of master daemon, showing alive workers, showing current queue of requests, showing membership list ID and exiting the master.

A list of available commands are shown as follows:
1. start master: command for starting master daemon, should be run only once to start the master.
2. show alive workers: show list of MapleJuice workers available to master.
3. show current requests: show list of requests queued at master.
4. show membership ID: show master membership list ID.
5. exit: exit master program.

### MapleJuice Worker
#### Overall Process for Starting Worker
> Before starting the workers, the master has to be started first.

```
python3 worker.py
```
All logs are stored in worker_log.log file in the same directory.

#### Compile and Run the Code
To run the worker.py, make a directory 'worker' on the location where worker.py is at.

### MapleJuice Maple/Juice Commandline Utilities 
#### Usage
maple.py is the client script for performing the maple operation on Maplejuice with the given maple_exe file. It connects with master.py to perform the maple.
The required exe file is not actually a executable file, but a python script that includes a function called maple(data), which expects the parameter to be a dictionary of key and list of values.
The expected return type of the maple function is a dictionary of key and list of values as well.
The commandline for running the maple.py is as following:

```
python3 maple.py <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
```

juice.py is the client script for performing the juice operation on Maplejuice with the given juice_exe file. It connects with master.py to perform the juice.
The expected values are the same as maple.py except that the name of function in juice script is juice(data).
The commandline for running the maple.py is as following:

```
python3 juice.py <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
```

### Hadoop
#### Overall Process for Running Hadoop Application
1. Set up a Hadoop cluster (including HDFS).
2. Compile the Hadoop application java codes.
3. Store input data to the HDFS.
4. Schedule Hadoop to run the application

#### Setting up Hadoop Cluster
1. Follow the guide [here](https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/?fbclid=IwAR0dQaSBktvTsG8TDHV6fMdGu6TE0wGWavaYXliPZjcSkVBh1UYD6UxazAU)

> Note that to not break Engineering IT VMs, do not change the /etc/hosts file on the VMs and use the hostname of each VM directly.

> The cluster size can be expanded to more than three nodes by adding more hostnames in the workers configuration file and setting up the Hadoop file locally at each VM.

#### Compile and Run the Code
1. Export variables for JAVA_HOME, PATH and HADOOP_CLASSPATH
2. Compile Hadoop Java code to create jar file by issuing
```
bin/hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
bin/hadoop com.sun.tools.javac.Main StreetCount.java
jar cf sc.jar StreetCount*.class
```

> Refer to [Hadoop MapReduce guide](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) for more details

#### Storing input to HDFS
> Assuming that HDFS is already running when setting up Hadoop cluster

To store input data in HDFS, use the following command:
```
hdfs dfs -put INPUT_FILENAME DIRECTORY
```
where ```INPUT_FILENAME``` is the local target file that should be stored in the HDFS, ```DIRECTORY``` is the HDFS directory to store files.

To create a directory in HDFS, use the following command:
```
hdfs dfs -mkdir DIRECTORY
```
where ```DIRECTORY``` is the HDFS directory being created in HDFS.

#### Using Hadoop to run the applications
> Assuming that yarn is already running when setting up Hadoop cluster

To run the WordCount application, use the following commands:
```
yar jar wc.jar WordCount INPUT_FILEPATH OUTPUT_DIR
```
where ```INPUT_FILEPATH``` is the filepath in HDFS to the input file. ```OUTPUT_DIR``` is a non-existent directory in HDFS to store the output results.

The application results will be stored in ```OUTPUT_DIR/part-r-00000```, use ```hdfs dfs -cat OUTPUT_DIR/part-r-00000``` to see the results.

To run the StreetCount application, use the following commands:
```
yar jar sc.jar StreetCount INPUT_FILEPATH OUTPUT_DIR
```
where ```INPUT_FILEPATH``` is the filepath in HDFS to the input file (in this case the Address_Points.csv stated in the report). ```OUTPUT_DIR``` is a non-existent directory in HDFS to store the output results.

The application results will be stored in ```OUTPUT_DIR/part-r-00000```, use ```hdfs dfs -cat OUTPUT_DIR/part-r-00000``` to see the results.

> Note that ```OUTPUT_DIR``` should be non-existent for the application to run correctly. Use ```hdfs dfs -rm -r OUTPUT_DIR``` to remove the output directory.
