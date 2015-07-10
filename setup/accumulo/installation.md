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

$ rm /tmp/* -rf 

### 2.1 Start HDFS

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