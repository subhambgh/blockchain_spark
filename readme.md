# CIS6930 – BlockChain
#### HW -2: Exploring Bitcoin Transactions

Dataset: Original dataset from https://senseable2015-6.mit.edu/bitcoin/ 

#### Technology Stack
```
Apache Spark v2.3.2 in Scala v2.11.12
Hadoop v3.0
Java v8
on AWS Educate Instance using EMR, S3 Modules
```

#### System Configuration
```
Master Node
EMR Instance Type	vCPU	ECU	Memory (GiB)**	Instance Storage (GB)**	Instance Count
m5.xlarge	4	16	16 GiB	96 GB	1

Worker Nodes
EMR Instance Type	vCPU	ECU	Memory (GiB)**	Instance Storage (GB)**	Instance Count
m5.xlarge	4	16	16 GiB	96 GB	7

Task Node (spot instance)
EMR Instance Type	vCPU	ECU	Memory (GiB)**	Instance Storage (GB)**	Instance Count
m5.xlarge	4	16	16 GiB	96 GB	1
** data specified above corresponds to available resources before spark and Hadoop installations
* HDFS was used with default replication factor i.e., 3
```

#### Main Class
```
1.	blockchain_spark/src/main/scala/com/blockchain/app/Part1_1.scala – Used for Part1. Q1 – Q4
2.	blockchain_spark/src/main/scala/com/blockchain/app/Part1_2.scala - Used for Part1. Q5 – Q8
3.	blockchain_spark/src/main/java/com/blockchain/app/PreProcessinginHDFS.java – Used to preprocess txin.dat and txout.dat
4.	blockchain_spark/src/main/scala/com/blockchain/app/PreProcForPart2.scala – Draws Graph and calculates the connected component analysis
5.	blockchain_spark/src/main/scala/com/blockchain/app/Part2.scala – Used for Part2
```

##### Compiled Using: sbt assembly
##### Run Time (approx): 15min (Part1) + 45min (Pre-Processing) + 12min (Part2) 
##### Note: 
```
1.	Can also be verified on a small dataset using the test configurations (files locations can be specified in resources/config-Local.properties file)
2.	Also, note that all the logic for part1 and part2 are specified as comments in the main classes above.
3.	Pre-processing was done on a single instance.
```

#### Idea behind Part II (the most interesting one)

#### Step1: Joint Control			 
```
1.	Firstly, draw a vertex for each address ID in addresses.dat. 
2.	Add an edge between address if they belong to the same transaction and store the tx information.		 
```

#### Step2: Serial Control
```
1.	Find all the single o/p transactions
2.	Now, for a single o/p transaction with txID say tx1, find the edge with same transaction ID as tx1 in the above graph.
3.	Connect the o/p address with one of the vertices belonging to that edge as shown below
```


