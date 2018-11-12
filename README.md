# Useful code to work with HBase


While working with HBase in CreditVidya, we come across different requirements which we found difficult to get an answer on the internet. The repo is built to share our such experience that might be helpful to developer come across under the same use case.


#### Issue: Multiple regions starting with the same key


The hbase .meta region become offline, Instead of making it online we end up corrupting it. So we follow offline meta repair using `org.apache.hadoop.hbase.util.hbck.OfflineMetaRepair` mapreduce job. It did recreate .meta but now we started observing issue like region inconsistency, a region overlapping and multiple regions starting with the same key issues. Most of the issue can resolve using `hbase hbck -fixAssignments -fixMeta tableName` cmd. But an issue like multiple regions starting with the same key was not getting resolved. So we come up with a solution based on merging region. But doing it through `hbase shell` was difficult for us until you are comfortable with writing ruby code using shell ruby wrappers. Instead, We used java client to identify affected region and iteratively merged affected region.

```
Java -jar org.cv.hbase.oneoff.MultRegionStartWithSameKeyIssue --help

Usage: MultRegionStartWithSameKeyIssue [options]

  Options:

    --help, -help
    --mergeRegion, -merge
      Merge regions starts with same key
      Default: false
    --retryAfterTimeInSecond, -rfts
      Merge regions starts with same key
      Default: 10
    -hbaseTableName, -tb
      HBase table name
    -maxRetries, -mr
      Set max retires, default is 3
      Default: 3
    -zkHosts, -hosts
      Comma-separated list of hostname for zookeeper quorum
      Default: 127.0.0.1
    -zkPath, -path
      Hbase parent path
      Default: /hbase
    -zkPort, -port
      Zookeeper port, default is 2181
      Default: 2181
```
To identity affected regions
```
Java -jar org.cv.hbase.oneoff.MultRegionStartWithSameKeyIssue -tb <ableName> -hosts <ip> 
hbase:meta
Repeating startKey [481b9, bfbf]
Total 30
startKey: 481b9, no of regions: 5
{ENCODED => 58ed6321262b7330925c30969ac1a7af, NAME => 'l0_apps_data,481b9,1539342766297.58ed6321262b7330925c30969ac1a7af.', STARTKEY => '481b9', ENDKEY => '4ee2d4'}
{ENCODED => 55969971996a0a387b0487eebd95ea9e, NAME => 'l0_apps_data,481b9,1538551346777.55969971996a0a387b0487eebd95ea9e.', STARTKEY => '481b9', ENDKEY => '55de85'}
{ENCODED => 12d08ccca3293d0690ffea0c5de0479b, NAME => 'l0_apps_data,481b9,1540443739200.12d08ccca3293d0690ffea0c5de0479b.', STARTKEY => '481b9', ENDKEY => '55de85'}
{ENCODED => a35863658995dee0a9663939938c5873, NAME => 'l0_apps_data,481b9,1540447095740.a35863658995dee0a9663939938c5873.', STARTKEY => '481b9', ENDKEY => '55de85'}
{ENCODED => f185fc4cddb5665d3f9cff2d28c1fc8e, NAME => 'l0_apps_data,481b9,1540447135584.f185fc4cddb5665d3f9cff2d28c1fc8e.', STARTKEY => '481b9', ENDKEY => '55de85'}
startKey: bfbf, no of regions: 4
{ENCODED => 5c4536fad980d7408494b6903c9ec8e1, NAME => 'l0_apps_data,bfbf,1539945091011.5c4536fad980d7408494b6903c9ec8e1.', STARTKEY => 'bfbf', ENDKEY => 'cfd095'}
{ENCODED => 6e7dda0d0d7e85a7c5fccbf3e8eb9f38, NAME => 'l0_apps_data,bfbf,1536303841618.6e7dda0d0d7e85a7c5fccbf3e8eb9f38.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}
{ENCODED => 9e1d653028c630eb419efd000a2a21fd, NAME => 'l0_apps_data,bfbf,1540382357842.9e1d653028c630eb419efd000a2a21fd.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}
{ENCODED => 2d545b3c41bf3bb2bc86bb4bd23d3cd7, NAME => 'l0_apps_data,bfbf,1540447115945.2d545b3c41bf3bb2bc86bb4bd23d3cd7.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}

startKey: 481b9, no of regions: 4
{ENCODED => 58ed6321262b7330925c30969ac1a7af, NAME => 'l0_apps_data,481b9,1539342766297.58ed6321262b7330925c30969ac1a7af.', STARTKEY => '481b9', ENDKEY => '4ee2d4'}
{ENCODED => 55969971996a0a387b0487eebd95ea9e, NAME => 'l0_apps_data,481b9,1538551346777.55969971996a0a387b0487eebd95ea9e.', STARTKEY => '481b9', ENDKEY => '55de85'}
{ENCODED => 12d08ccca3293d0690ffea0c5de0479b, NAME => 'l0_apps_data,481b9,1540443739200.12d08ccca3293d0690ffea0c5de0479b.', STARTKEY => '481b9', ENDKEY => '55de85'}
{ENCODED => 41de297149a6e307c3dcc5ca42613430, NAME => 'l0_apps_data,481b9,1540552041321.41de297149a6e307c3dcc5ca42613430.', STARTKEY => '481b9', ENDKEY => '55de85'}
startKey: bfbf, no of regions: 4
{ENCODED => 5c4536fad980d7408494b6903c9ec8e1, NAME => 'l0_apps_data,bfbf,1539945091011.5c4536fad980d7408494b6903c9ec8e1.', STARTKEY => 'bfbf', ENDKEY => 'cfd095'}
{ENCODED => 6e7dda0d0d7e85a7c5fccbf3e8eb9f38, NAME => 'l0_apps_data,bfbf,1536303841618.6e7dda0d0d7e85a7c5fccbf3e8eb9f38.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}
{ENCODED => 9e1d653028c630eb419efd000a2a21fd, NAME => 'l0_apps_data,bfbf,1540382357842.9e1d653028c630eb419efd000a2a21fd.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}
{ENCODED => 2d545b3c41bf3bb2bc86bb4bd23d3cd7, NAME => 'l0_apps_data,bfbf,1540447115945.2d545b3c41bf3bb2bc86bb4bd23d3cd7.', STARTKEY => 'bfbf', ENDKEY => 'dfea176fe1d66b2b9b8db8b50163m'}
```
To fix the issue, just pass -merge
```
Java -jar org.cv.hbase.oneoff.MultRegionStartWithSameKeyIssue -tb <ableName> -hosts <ip> -merge
```

![Iterative Regio merge strategy](https://nabhosal.github.io/external-images/multi-region-strt-with-sameKey.png)

Reference:
https://stackoverflow.com/questions/24584174/manual-fix-of-hbase-table-overlap-multi-region-has-same-start-key
https://blog.csdn.net/microGP/article/details/81233132
