package com.xingcloud.hbase.reader;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.store.RecordReader;
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
    public void testRecordReader(){
        HBaseScanPOP.ScanType[] types=new HBaseScanPOP.ScanType[4];
        types[0]=new HBaseScanPOP.ScanType("day", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED);
        types[1]=new HBaseScanPOP.ScanType("event", SchemaDefProtos.MinorType.VARCHAR4, SchemaDefProtos.DataMode.REQUIRED);
        types[2]=new HBaseScanPOP.ScanType("uid", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED);
        types[3]=new HBaseScanPOP.ScanType("val", SchemaDefProtos.MinorType.UINT8, SchemaDefProtos.DataMode.REQUIRED);
        String eventPattern="visit.*";
        List<String> dayList=new ArrayList<>();
        dayList.add("20121201");
        //dayList.add("20121203");
        String pID="sof-dsk";
        HBaseScanPOP.HBaseScanEntry entry=new HBaseScanPOP.HBaseScanEntry(types,dayList,eventPattern,pID);
        HBaseRecordReader reader=new HBaseRecordReader(null,entry);
        List<RecordReader> readerList=new ArrayList<RecordReader>();
        readerList.add(reader);
        Iterator<RecordReader> iter=readerList.iterator();
        try {
            ScanBatch batch=new ScanBatch(null,iter);
            /*
            while(batch.next()!= RecordBatch.IterOutcome.NONE){
                for(MaterializedField f : batch.getSchema()){
                    ValueVector v = batch.getValueVector(f.getFieldId(), TypeHelper.getValueVectorClass(f.getType().getMinorType(), SchemaDefProtos.DataMode.REQUIRED));
                    for(int i = 0 ; i < v.getRecordCount() ; i ++){
                        System.out.print(v.getObject(i) + " ");
                    }
                    System.out.println();

                }
            }
            */
        } catch (ExecutionSetupException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        int recordnum=0;

        while((recordnum=reader.next())!=0){

            System.out.println("success "+recordnum);
        }


    }
}
