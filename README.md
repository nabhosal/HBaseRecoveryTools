# Useful code to work with HBase


While working with HBase in CreditVidya, we come across different requirements which we found difficult to get an answer on the internet. The repo is built to share our such experience that might be helpful to developer come across under the same use case.

### Issue: Multiple regions starting with the same key

The hbase .meta region become offline, Instead of making it online we end up corrupting it. So we follow offline meta repair using `org.apache.hadoop.hbase.util.hbck.OfflineMetaRepair` mapreduce job. It did recreate .meta but now we started observing issue like region inconsistency, a region overlapping and multiple regions starting with the same key issues. Most of the issue can resolve using `hbase hbck -fixAssignments -fixMeta tableName` cmd. But an issue like multiple regions starting with the same key was not getting resolved. So we come up with a solution based on merging region. But doing it through `hbase shell` was difficult for us until you are comfortable with writing ruby code using shell ruby wrappers. Instead, We used java client to identify affected region and iteratively merged affected region.

[Resolution in details](doc/multi-region-start-with-sameKey.md)

### Java code for reading HBase exported sequence
We used HBase export utility to take daily dump/backup of HBase tables. Dump data is further ingested to s3 to make it available for data science (i.e. DS) team. Since DS team works with a subset of data where the subset is not defined by time range. We need to read dump data and create segregated data such that it is usable by DS team. Data segregation can be achieved through spark, but being startup company we can’t afford to run continuous spark job just for data ingestion, we need something standalone that can run on spot instances and need not to be a spark based.

[How to read Hbase sequence through Java](doc/read-hbase-sequence-file.md)