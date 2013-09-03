package org.apache.drill.exec.physical.impl;

import com.xingcloud.meta.ByteUtils;
import org.apache.drill.exec.physical.impl.unionedscan.MultiEntryHBaseRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/3/13
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHBaseRecordReader {
    @Test
    public void testRk(){
        String srk="20130101visit.b.c";
        String enk="20130101visit";
        byte[] srkBytes= MultiEntryHBaseRecordReader.appendBytes(ByteUtils.toBytesBinary(srk), MultiEntryHBaseRecordReader.produceTail(true));
        byte[] enkBytes= MultiEntryHBaseRecordReader.appendBytes(ByteUtils.toBytesBinary(enk),MultiEntryHBaseRecordReader.produceTail(false));
        Pair<Long,Long> pair= getStartEndUidPair();
        byte[] sr= Bytes.toBytes(pair.getFirst());
        byte[] en= Bytes.toBytes(pair.getSecond());
        System.out.println("  ");
    }

    public static Pair<Long, Long> getStartEndUidPair() {
        long startUid = 0l << 32;
        long endUid = (1l << 40) - 1l;

        return new Pair<Long, Long>(startUid, endUid);
    }
}
