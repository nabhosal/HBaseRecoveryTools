package org.cv.hbase.oneoff.test;

import com.beust.jcommander.JCommander;
import org.cv.hbase.oneoff.MultRegionStartWithSameKeyIssue;
import org.junit.Test;

/**
 * Created by nilesh on 31/10/18.
 */
public class CMDArgsTest {

    @Test
    public void basicTest(){

        MultRegionStartWithSameKeyIssue.CMDArgs args = new MultRegionStartWithSameKeyIssue.CMDArgs();
        String[] argv = { "-zkHosts", "localhost", "-tb", "l0_sms_data" , "-merge"};
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        System.out.println(args);

    }

    @Test
    public void withDefaultArgsTest(){

        MultRegionStartWithSameKeyIssue.CMDArgs args = new MultRegionStartWithSameKeyIssue.CMDArgs();
        String[] argv = {  };
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        System.out.println(args);

    }

    @Test
    public void withPrintReportOnlyTest(){

        MultRegionStartWithSameKeyIssue.CMDArgs args = new MultRegionStartWithSameKeyIssue.CMDArgs();
        String[] argv = { "-hosts", "localhost", "-tb", "l0_sms_data" };
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        System.out.println(args);

    }

    @Test
    public void withParseArgsTest(){

        String[] argv = { "-hosts", "localhost", "-tb", "l0_sms_data" };
        System.out.println(MultRegionStartWithSameKeyIssue.CMDArgs.parseArgs(argv));
    }
}
