package org.apache.drill.exec.store;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.HbaseUserScanPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.record.vector.VarLen4;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/10/13
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHBaseRecordReader {
    @Test
    public void testRecordReader() {
        long startTime = System.currentTimeMillis();
        long recordCount = 0 ;
        String eventPattern = "*.*";
        String startday = "20121201";
        String endDay="20121201";
        String pID = "testtable100W";
        HbaseScanPOP.HbaseScanEntry entry = new HbaseScanPOP.HbaseScanEntry(pID, startday, endDay, eventPattern);
        HBaseRecordReader reader = new HBaseRecordReader(null, entry);
        List<RecordReader> readerList = new ArrayList<RecordReader>();
        readerList.add(reader);
        Iterator<RecordReader> iter = readerList.iterator();
        try {
            ScanBatch batch = new ScanBatch(null, iter);
            long ts1,ts2;
            ts1=System.currentTimeMillis();
            while (batch.next() != RecordBatch.IterOutcome.NONE) {
                recordCount += batch.getRecordCount();
                ts2=System.currentTimeMillis();
                System.out.println(recordCount+": "+batch.getRecordCount()+" costs "+(ts2-ts1)+"ms");
                ts1=ts2;

                for (MaterializedField f : batch.getSchema()) {
                    if(!f.getName().equals("uid")&&!f.getName().equals("event"))continue;
                    ValueVector v = batch.getValueVector(f.getFieldId());
                    System.out.print(f.getName() + ":");
                    if (v instanceof VarLen4) {
                        for (int i = 0; i < v.getRecordCount(); i++) {
                            System.out.print(new String((byte[]) v.getObject(i)) + " ");
                        }
                    } else {
                        for (int i = 0; i < v.getRecordCount(); i++) {
                            System.out.print(v.getObject(i) + " ");
                        }
                    }
                    System.out.println();
                }
            }
        } catch (ExecutionSetupException e) {
            e.printStackTrace();
        }
        System.out.println("Done , recordCount :" +  recordCount + ", cost time " + (System.currentTimeMillis() - startTime)/1000 + " seconds");
    }
    @Test
    public void testUserTable(){
        long startTime = System.currentTimeMillis();
        int recordCount = 0;
        String property="language";
        String val="null";
        String project_id="sof-dsk";
        HbaseUserScanPOP.HbaseUserScanEntry entry=new HbaseUserScanPOP.HbaseUserScanEntry(project_id,property,val);
        HBaseUserRecordReader reader=new HBaseUserRecordReader(null,entry);
        List<RecordReader> readerList = new ArrayList<RecordReader>();
        readerList.add(reader);
        Iterator<RecordReader> iter = readerList.iterator();
        try {
            ScanBatch batch = new ScanBatch(null, iter);
            while (batch.next() != RecordBatch.IterOutcome.NONE) {
                recordCount += batch.getRecordCount() ;
                for (MaterializedField f : batch.getSchema()) {
                    /*
                    ValueVector v = batch.getValueVector(f.getFieldId());
                    System.out.print(f.getName() + ":");
                    if (v instanceof VarLen4) {
                        for (int i = 0; i < v.getRecordCount(); i++) {
                            System.out.print(new String((byte[]) v.getObject(i)) + " ");
                        }
                    } else {
                        for (int i = 0; i < v.getRecordCount(); i++) {
                            System.out.print(v.getObject(i) + " ");
                        }
                    }
                    System.out.println(); */
                }
            }
        } catch (ExecutionSetupException e) {
            e.printStackTrace();
        }
        System.out.println("Done , recordCount :" +  recordCount + ", cost time " + (System.currentTimeMillis() - startTime)/1000 + " seconds");

    }
}