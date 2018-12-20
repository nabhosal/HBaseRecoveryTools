package org.cv.hbase.oneoff.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Created by nilesh on 2/11/18.
 */
public class ReadHBaseSequenceFile {

    static final Logger LOG = LoggerFactory.getLogger(ReadHBaseSequenceFile.class);

    public static void main(String ...args){

        Configuration conf = new Configuration();
        conf.setStrings(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY, conf.get(CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY),
                ResultSerialization.class.getName(),
                WritableSerialization.class.getName()
        );
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
//            Path inputPath = new Path("./hbase-sequence-file-reader/src/main/resources/sequencefiles/part-m-00000");
            Path inputPath = new Path("/home/nilesh/cv_dump/part-m-00007");
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);

            WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();

            Result result = null;

            while (reader.next(key)){
                String skey = Bytes.toString(((ImmutableBytesWritable)key).get());
                result = (Result) reader.getCurrentValue(result);
                NavigableMap<byte[], byte[]> resultMap = result.getFamilyMap(Bytes.toBytes("d"));
                System.out.println(skey);
                resultMap.forEach((k, v) -> {
                    System.out.println(Bytes.toString(k) +" "+Bytes.toString(v));
                });
            }
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

    }
}
