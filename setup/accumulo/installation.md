Accumulo Installation
=====================

1. Prerequisite
---------------

### 1.1 HDFS

[HDFS Doc]

### 1.2 Zookeeper 

### 1.3 Accumulo

[Accumulo Doc]

2. Setup
--------

### 2.0 Remove HDFS meta- and actual data (by default in /tmp)

Only do this before running HDFS the first time.
Don't do it later because you loose meta- and actual HDFS data.

$ rm /tmp/* -rf 

### 2.1 Start HDFS and create HDFS folder for accumulo

$ sbin/start-dfs.sh

$ bin/hdfs dfs -mkdir /accumulo

### 2.2 Start ZK

```
$ bin/zkServer.sh start
```
 
### 2.3 Start Accumulo

bin/accumulo init 
instance: plenario 
passwd for root: plenario 

conf

master 0.0.0.0
monitor 0.0.0.0

```
$ bin/start-all.sh 
```

To enable client connection, such as python API. 

```
$ bin/accumulo proxy -p proxy/proxy.properties
```

Sample client code. 

```python
from pyaccumulo import Accumulo, Mutation, Range 
conn = Accumulo(host="my.proxy.hostname", port=50096, user="root", password="secret")
```

[HDFS Doc]:http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-common/SingleCluster.html
[Accumulo Doc]:https://github.com/apache/accumulo/blob/master/INSTALL.md

### Stop everything

First stop accumulo

$ bin/stop-all.sh

Next the zookeeper:

$ bin/zkServer.sh stop

Finally, HDFS:

$ sbin/stop-dfs.sh


