
# CIS6930 – BlockChain
#### HW -2: Exploring Bitcoin Transactions
![Bitcoin Transactions](/bt.png)

**Project Description**: The projet comprised of two parts and we had to analyze approximately 5 lakh blocks of the orignal dataset comprising a total of over 100 GB of data.

**Part 1: Data Analysis** 
Calcualte balance per address (UTXO's) - Each Bitcoin transaction has atleast one I/P and one O/P and the I/P's and O/P's are wrt the transactions (not users). Each I/P is spent and O/P is received for a user. So UTXO's are the amount of unspent transactions, until a later I/P spents it. This was calculated as  (sum of O/P transactions for a user - sum of I/P transactions for that user)
  
**Part 2: Heuristics**
  2.1 Joint Control - where we had to assume all the I/P address of a transaction is controlled by a single user.
  2.2 Serial Control - where all the O/P transaction with only a single O/P is controlled by the same user owning the I/P address.

**Dataset**: Original dataset from https://senseable2015-6.mit.edu/bitcoin/ 

**Technology Stack**
Apache Spark v2.3.2 in Scala v2.11.12
Hadoop v3.0
Java v8
on AWS Educate Instance using EMR, S3 Modules

**Part II**
***Step1***: Joint Control			 
1. Firstly, draw a vertex for each address ID in addresses.dat. 
2. Add an edge between address if they belong to the same transaction and store the tx information.		 

***Step2***: Serial Control
1. Find all the single o/p transactions
2. Now, for a single o/p transaction with txID say tx1, find the edge with same transaction ID as tx1 in the above graph.
3. Connect the o/p address with one of the vertices belonging to that edge as shown below

***Step 3***: Calculate the connected component analysis on the formed graph using BFS/DFS
1. All the connected component will belong to a single user.


**Joint Control & Serial Control**
![Joint Control](/jcsc.png)         


**System Configuration**
EMR Instance Type	| vCPU	| ECU | 	Memory (GiB)** |	Instance Storage (GB)** |	Instance Count | Node type
--- | --- | --- | --- |--- |--- |---
m5.xlarge	| 4	| 16 |	16 GiB |	96 GB |	1 | Master
m5.xlarge |	4	| 16	| 16 GiB	| 96 GB |	7 | Worker
m5.xlarge	| 4	| 16 |	16 GiB |	96 GB |	1 | Task

** data specified above corresponds to available resources before spark and Hadoop installations
* HDFS was used with default replication factor i.e., 3

#### Main Class
1. blockchain_spark/src/main/scala/com/blockchain/app/Part1_1.scala – Used for Part1. Q1 – Q4
2. blockchain_spark/src/main/scala/com/blockchain/app/Part1_2.scala - Used for Part1. Q5 – Q8
3. blockchain_spark/src/main/java/com/blockchain/app/PreProcessinginHDFS.java – Used to preprocess txin.dat and txout.dat
4. blockchain_spark/src/main/scala/com/blockchain/app/PreProcForPart2.scala – Draws Graph and calculates the connected component analysis
5. blockchain_spark/src/main/scala/com/blockchain/app/Part2.scala – Used for Part2

**Run Time (approx)**: 15min (Part1) + 45min (Pre-Processing) + 12min (Part2) 

Note:
1. Can also be verified on a small dataset using the test configurations (files locations can be specified in resources/config-Local.properties file)
2. Also, note that all the logic for part1 and part2 are specified as comments in the main classes above.
3. Pre-processing was done on a single instance.


