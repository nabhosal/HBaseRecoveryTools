package org.cv.hbase.oneoff;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by nilesh on 29/10/18.
 */
public class MultRegionStartWithSameKeyIssue {


    public static class CMDArgs {

        @Parameter(names = {"-zkHosts", "-hosts"}, description = "Comma-separated list of hostname for zookeeper quorum")
        public String zkQuorum = "127.0.0.1";

        @Parameter(names = {"-zkPort", "-port"}, description = "Zookeeper port, default is 2181")
        public String zkPort = "2181";

        @Parameter(names = {"-zkPath", "-path"}, description = "Hbase parent path")
        public String zkParentPath = "/hbase";

        @Parameter(names = {"-hbaseTableName", "-tb"}, description = "HBase table name")
        public String tableName;

        @Parameter(names = {"-maxRetries", "-mr"}, description = "Set max retires, default is 3")
        public int maxRetries = 3;

        @Parameter(names = {"--mergeRegion", "-merge"}, description = "Merge regions starts with same key")
        public boolean merge = false;

        @Parameter(names = {"--retryAfterTimeInSecond", "-rfts"}, description = "Merge regions starts with same key")
        public int retryAfterTimeInSecond = 10;

        @Override
        public String toString() {
            return "CMDArgs{" +
                    "zkQuorum='" + zkQuorum + '\'' +
                    ", zkPort='" + zkPort + '\'' +
                    ", zkParentPath='" + zkParentPath + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", maxRetries=" + maxRetries +
                    ", merge=" + merge +
                    ", retryAfterTimeInSecond=" + retryAfterTimeInSecond +
                    '}';
        }

        public static CMDArgs parseArgs(String ...argv){

            MultRegionStartWithSameKeyIssue.CMDArgs args = new MultRegionStartWithSameKeyIssue.CMDArgs();
            JCommander.newBuilder()
                    .addObject(args)
                    .build()
                    .parse(argv);

            return args;
        }
    }

    public static void main(String []args) throws IOException, InterruptedException {


        MultRegionStartWithSameKeyIssue.CMDArgs config = MultRegionStartWithSameKeyIssue.CMDArgs.parseArgs(args);
        System.out.println("Running with args \n"+config);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", config.zkPort);
        conf.set("hbase.zookeeper.quorum", config.zkQuorum);
        conf.set("zookeeper.znode.parent", config.zkParentPath);
        Connection client = ConnectionFactory.createConnection(conf);
        System.out.println("------- Connection established ------> ");
        int maxRegionsPending;
        int maxRetries = config.maxRetries;
        boolean dontMerge = !config.merge;
        do{
            maxRegionsPending = scanAndMergeRegionWithSameKeys(client, TableName.valueOf(config.tableName), dontMerge);
            maxRetries--;
            System.out.println("maxRegionsPending "+maxRegionsPending);
            TimeUnit.SECONDS.sleep(config.retryAfterTimeInSecond);
        }while(maxRegionsPending != 0 && maxRetries > 0 && !dontMerge);

    }

    private static int scanAndMergeRegionWithSameKeys(Connection client, TableName tableName, boolean dontMerge) throws IOException {

        NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(client, tableName);
        System.out.println(TableName.META_TABLE_NAME.getNameAsString());

        Set<String> repeatingStartKey = new HashSet<>();
        Map<String, List<HRegionInfo>> startKeyWiseRegions = new LinkedHashMap<>();

        regions.forEach((hRegionInfo, serverName) -> {

            String decodedStartKey = Bytes.toString(hRegionInfo.getStartKey());
            if( !startKeyWiseRegions.containsKey(decodedStartKey)){
                startKeyWiseRegions.put(decodedStartKey, new LinkedList<HRegionInfo>());
            }else
                repeatingStartKey.add(decodedStartKey);
            startKeyWiseRegions.get(decodedStartKey).add(hRegionInfo);

        });

        System.out.println("Repeating startKey "+repeatingStartKey);
        System.out.println("Total "+regions.size());
        AtomicInteger ordinal = new AtomicInteger(0); // TODO : BAD HACK
        repeatingStartKey.forEach( startKey -> {
            System.out.println("startKey: "+startKey +", no of regions: "+startKeyWiseRegions.get(startKey).size());
            startKeyWiseRegions.get(startKey).forEach(System.out::println);
            /* special case when only 2 region are pending */
            ordinal.set(Integer.max(ordinal.get(), startKeyWiseRegions.get(startKey).size()));
            if( !dontMerge) {
                if (startKeyWiseRegions.get(startKey).size() == 2)
                    mergeLastTwoRegions(startKeyWiseRegions.get(startKey), client);
                else
                    mergeRegionsWithSameKey(startKeyWiseRegions.get(startKey), client);
            }
        });

        return ordinal.get();
    }

    private static void mergeLastTwoRegions(List<HRegionInfo> hRegionInfos, Connection client){

        System.out.println("mergeLastTwoRegions");
        HRegionInfo firstRegion = hRegionInfos.get(0);
        HRegionInfo secondRegion = hRegionInfos.get(1);

        System.out.println("Merging below 2 regions, 1st: "+firstRegion.getEncodedName()+", 2nd: "+secondRegion.getEncodedName());
        System.out.println("First Region "+firstRegion);
        System.out.println("Second Region "+secondRegion);
        try {
            client.getAdmin().mergeRegions(firstRegion.getEncodedNameAsBytes(), secondRegion.getEncodedNameAsBytes(), true);
            TimeUnit.SECONDS.sleep(5);

        } catch (IOException e) {
            System.out.println("Failed while merging ");
            System.out.println("First Region "+firstRegion);
            System.out.println("Second Region "+secondRegion);
            e.printStackTrace();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void mergeRegionsWithSameKey(List<HRegionInfo> hRegionInfos, Connection client){

        /* No Merge required */
        if(hRegionInfos.size() == 1){
            return ;
        }
        System.out.println("Create sublist without first region");
        List<HRegionInfo> hRegionInfosSet = hRegionInfos.subList(1, hRegionInfos.size());
        Collections.reverse(hRegionInfosSet);

        int numOfMergeGroups = (int)((hRegionInfosSet.size() / 2 ));
        int index = 0;

        for(int grp = 1 ; grp <= numOfMergeGroups ; grp++){

            HRegionInfo firstRegion = hRegionInfosSet.get(index++);
            HRegionInfo secondRegion = hRegionInfosSet.get(index++);

            System.out.println("Merging below 2 regions, 1st: "+firstRegion.getEncodedName()+", 2nd: "+secondRegion.getEncodedName());
            System.out.println("First Region "+firstRegion);
            System.out.println("Second Region "+secondRegion);

            try {
                client.getAdmin().mergeRegions(firstRegion.getEncodedNameAsBytes(), secondRegion.getEncodedNameAsBytes(), true);
                TimeUnit.SECONDS.sleep(5);

            } catch (IOException e) {
                System.out.println("Failed while merging ");
                System.out.println("First Region "+firstRegion);
                System.out.println("Second Region "+secondRegion);
                e.printStackTrace();
            }catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
