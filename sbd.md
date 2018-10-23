**Results of running our Spark program on AWS Cloud**
**Group 17**
**Antal Száva, Maryam Tavakkoli**

***1- How to read data segments from AWS S3 buckets***
In order to make sure that no exceptions arise while taking arguments for reading the dataset, we changed how we give the path.
So, we added the below function:

```
def parse_args (args: Array[String]): String ={
   if (args(0) == "--segments")
     return args(1)
   else if (!args.isEmpty)
     throw new IllegalArgumentException()
   else
     return "./*.csv"
 }
```

Then to read a part of the dataset, we are using the below line in the arguments while creating a step in the cluster.
Then, we just set the name of csv files based on the amount that we wanted to try.
For example, we first tried to run around 2800 segments of data like below:

```
  --segments s3://gdelt-open-data/v2/gkg/201502\*.gkg.csv
```

***2- Test cases with small segment numbers***
In the first step, we tried different test cases with small segment
numbers in order to compare metrics such as different number of
segments, type and power of nodes, number of worker nodes and type of
Scala program (RDD or dataframe). Results of these measurements are
shown in the table 1.

| Test  | Master Type | Core Type | Number of Worker (core) Nodes | Number of Segments|Run time|Price|Percentage Increase|Scala Program|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1  | M4.large  | M4.large |4|2800|7min|0.0110|-|DF|
| 2  | M4.large  | M4.large |4|8900|17min|0.0269|144%|DF|
|3 | M4.large | C4.2xlarge |4|2800|2min|0.0102|-|DF|
|4| M4.large  | C4.2xlarge |4|8900|4min|0.0205|101%|DF|
|5| M4.large  | C4.2xlarge |4|8900|5min|0.0257|-|RDD|
|6| M4.large  | M4.large |8|8900|9min|0.0256|-|DF|
|7| M4.large  | M4.large |8|8900|10min|0.0285|-|RDD|

 *Table 1. Results of different test cases*

From this table, is visible that:
-   **More segments:** Runtime increasing while keeping hardware
    characteristics the same, depends on the worker node's type and
    power. Increasing form 2800 to 8900 while running with M4.large
    cores causes a change in runtime from 7 minutes to 17 minutes, while
    it was from 2 minutes to 4 minutes running with C4.2xlarge core
    nodes.
-   **Type & power of nodes:** It causes lower price while it also
    decreases the run time. The percentage increase of price for scaling
    dataset from 2800 segments of data to 8900 segments was 144% for the
    first case (running with M4.large) while it was 101% for the second
    case (running with C4.2xlarge).
-   **Number of worker nodes:** Running with 8 M4.large core nodes
    instead of 4, for running 8900 segments give a lower runtime as well
    as lower price.
-   **Type of Scala program:** Results of running with RDD (instead of
    DF) showed one minutes delay (Repeated for two test cases).

***3- Run the entire dataset***
we used our RDD program for this purpose. We had memory limitation error
at the first which it solved with adding some configurations as below to
the cluster:

```
[
{"classification":"spark",
"properties":{"maximizeResourceAllocation":"true"}
}
]
```

With these settings we could run whole of dataset in 11 minutes using 20
c4.8xlarge core nodes and one c4.8xlarge master node. Regarding price of
used spot instances as \$0.29 per Hour, it would cost:

```
(0.29*21*11minutes)/60minutes = 1.1165$
```
Note: Running steps in client mode and cluster mode did not affect
runtime.
Looking at Ganglia while running with the above situation showed average
utilization of 41%.

***4. Try for Improvements***

***4.1. Lower Price***
-   ***Different Master Node Type***
Looking at performance of nodes in Ganglia, we noticed that one node is
not utilized as much as others which is probably the master node. Hence,
we ran the same measurements with a less powerful master node such as
C4.4xlarge and results were the same with 11 minutes run time. So this
way we are spending less money while we can achieve the same result.

-   ***Different Availability Zones***
We also noticed another fact about the price. While creating cluster and
step, there is a choice for EC2 subnet, including us-east-2a,
us-east-2b, us-east-2c.
Price history for instances depicts difference in these different
availability zones and it can be beneficial to choose the lowest at the
moment.

-   ***Number of Worker Nodes***
To see the effect of the number of worker nodes on the runtime and performance,
we tried to run the entire dataset with 15 C4.8xlarge core nodes and one
C4.4xlarge master node with the below configurations:
``
--executor-cores 5 --num-executors 104 --executor-memory 8g
``
Even though the runtime reached 12 minutes which compared to running
with 20 worker nodes is two minutes more, it would be still beneficial
regarding the price which is 0.10\$ cheaper in this case. This can be
shown by calculating the total price using these two conditions:

-   C4.4xlarge Master and 20 C4.8xlarge worker nodes with 10 minutes as
    runtime:
``
    (0.14+[0.29*20])*10minutes)/60minutes = 0.99$
``
-   C4.4xlarge Master and 15 C4.8xlarge worker nodes with 12 minutes as
    runtime:
``
    (0.14+[0.29*15])*12minutes)/60minutes = 0.89$
``

***4.2. Lower Runtime***

***4.2.1. Impact of Different Spark Configurations***
An executor is the smallest processing unit for completing tasks and
tasks are "smallest individual units of execution" in a Spark system
\[1\]\[2\]. Spark allows the resource customization of such executors.
We can specify the following three features:
-   **Number of executors**

-   **Number of cores per executor**

-   **Amount of memory per executor**

Also apart from our executors running our tasks, there are going to be
certain daemons running as well. For this reason, we need to allow them
enough computation space and so allocate executors for them.
Regarding number of cores, there is a suggested value of **5 cores per
executor** because of the throughput of the architecture of the HDFS
file system.
We can see that through the calibration of these three characteristics
we can achieve various running outcomes.

-   ***Few Executors:***
When having too few executors such as only one per node, then the
processing will have a bottleneck at the HDFS throughput and too much
garbage is going to be generated.

-   ***Too many Executors***
Going with the other extreme approach of having the most number of
executors such as one executor per core, our JVM is not going to be
utilised enough as there are going to be too many isolated environments
(cannot run enough tasks in the same JVM) and also not leaving enough
memory overhead for Hadoop/Yarn daemon processes \[3\].

-   ***Balanced Approach***
An approach in between the previous two was used during our tests. Based on
the sum of virtual cores of our instances in the cluster, we calculated
how many executors we could have (left one core per node for the
Application Manager). We assigned memory based on the number of
executors and calculated with a 7% overhead as well \[3\].

***Calculations:***
-   **\--executor-cores 5**
-   We have 20 instances with 36 Vcores, leave 1 core per node for the
    ApplicationManager -\> 36-1=35
- Total available of cores in cluster = 35 x 20 = 700
-   Number of total executors = (total cores in cluster/number of cores
    per executor) = 700/5 = 140
-   We will leave one executor for ApplicationManager =\>
    **\--num-executors 139**
-   Number of executors per node = 140/20 = 7

-   Memory per executor = 60GB/7 = 8.5GB

-   Counting off heap overhead = 7% of 7GB = 0.5GB. So, actual memory =
    8.5 - 0.5 = 8GB -\> **\--executor-memory 8**

Therefore, we run cluster with the below line specifications and we
achieved a **10 minute runtime** for the entire dataset.
``
--executor-cores 5 --num-executors 139 --executor-memory 8g
``

***Another Test:***

We have also tried resourcing with 3 and 4 executor cores with the below
specifications, but they did not make any change in the overall runtime.

``
--executor-cores 3 --num-executors 232 --executor-memory 5g
``
``
--executor-cores 4 --num-executors 174 --executor-memory 6g
``

***4.2.2. Impact of Different EC2 Instances***

***Memory intensive clusters***

We also tried to see how memory intensive instances work compared to
compute optimized C4 instances. To this goal, we chose one r4.4xlarge
node as a master and 20 r5.4xlarge nodes. Each of worker nodes has 16
vcores and 128 GB of memory. Figure 10 shows this test. The execution
time was 18 minutes and specifications were as follow:

``
--num-executors 59 --executor-cores 5 --executor-memory 40g.
``

***5. Additional measurements which we could not get positive
results:***

-   ***Level of Parallelism:*** We tried to set this property
    (*spark.default.parallelism*) for spark and since in general there
    is a recommendation for 2-3 tasks per CPU core in cluster, we tried
    it for 720, 1080 and 1440 but we did not see better results and
    runtime stayed at 10 minutes.

-   ***Data Serialization:*** We also applied property of serialization
    with the follow configuration but it did not help.
``
*conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")*
``
-   ***Optimize file sizes:*** We also tried to optimize file sizes. If
    files are too small (generally less than 128 MB), the execution
    engine might be spending additional time with the overhead of
    opening Amazon S3 files, listing directories, getting object
    metadata, setting up data transfer, reading file headers, reading
    compression dictionaries, and so on. On the other hand, if file is
    not splittable and the files are too large, the query processing
    waits until a single reader has completed reading the entire file.
    That can reduce parallelism \[4\]. Hence, we tried to use the
    **S3DistCP** utility on Amazon EMR to combine smaller files into
    larger objects. So, we created the below jar file and added a step
    but there was an error for running it and we could not find the
    problem.

``
[
    {
        "Name":"S3DistCp step",
        "Args":["s3-dist-cp","--s3Endpoint=s3.amazonaws.com","--src=s3://gdelt-open-data/v2/gkg/*.gkg","--dest=s3://bigdatagkg","--srcPattern=.*[a-zA-Z,]+"],
        "ActionOnFailure":"CONTINUE",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"    	
    }
]
``

***References:***
\[1\]https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-Executor.html
\[2\]https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-taskscheduler-Task.html
\[3\]https://spoddutur.github.io/spark-notes/distribution\_of\_executors\_cores\_and\_memory\_for\_spark\_application.html
\[4\]https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/
