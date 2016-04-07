# hdfsstats

Hdfs stats is library + tool to produce statsitics of hdfs directories for your cluster. It can produce reports which look like follolwing 

```javascript
HdfsStats{cluster='foo.bar', path='/user/baz', owner='hdfs', loadTime=2016-04-07T19:39:27.138Z, fileCount=12, dirCount=530, spaceConsumed=1343956, size=448084, accessCount=0}
HdfsStats{cluster='foo.bar', path='/user/baz/foo', owner='hdfs', loadTime=2016-04-07T19:39:27.138Z, fileCount=0, dirCount=523, spaceConsumed=0, size=0, accessCount=0}
HdfsStats{cluster='foo.bar', path='/user/mapred', owner='mapred', loadTime=2016-04-07T19:39:27.138Z, fileCount=388, dirCount=227, spaceConsumed=83511081, size=27837027, accessCount=0}
HdfsStats{cluster='foo.bar', path='/user/mapred/done', owner='mapred', loadTime=2016-04-07T19:39:27.138Z, fileCount=388, dirCount=222, spaceConsumed=83511081, size=27837027, accessCount=0}
```

It has two primary building blocks 

1. HdfsStatsBuilder : Polls the namenode and prepare a snapshot of current state of hdfs directories, for each directory stats are captured into a model HdfsStats. HdfsStats for a directory includes details such as size, number of files, number of directories, accress count, owner etc. 
2. HdfsStatsPublisher: Publishes the stats produced by the builder. A console publisher is provided, but you can chose to publish stats in any way you like (ex: save to RDBMS) 


These two together can provide a way to track evolution of an HDFS install and many interesting things can be done on top of timeseries thus produced. Ex: automated anomaly growth detection, data retention policies, monitoring & alerting etc. 
