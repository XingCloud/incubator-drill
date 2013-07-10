package com.xingcloud.hbase.reader;

import io.netty.buffer.ByteBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.*;
import org.apache.drill.exec.store.RecordReader;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/2/13
 * Time: 8:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseRecordReader implements RecordReader {
    static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

    private byte[] StartKey;
    private byte[] EndKey;
    private List<TableScanner> scanners = new ArrayList<TableScanner>();
    private int currentScannerIndex = 0;
    private List<KeyValue> curRes = new ArrayList<KeyValue>();
    private int valIndex = -1;
    private boolean hasMore;
    private SchemaPath rootPath;
    private OutputMutator output;
    private HBaseScanPOP.HBaseScanEntry config;
    private FragmentContext context;
    private ValueVector<?>[] valueVectors;
    private long recordsRead;
    private int BATCHRECORDCOUNT=1024;
    private Filter filter;

    public HBaseRecordReader(HBaseScanPOP.HBaseScanEntry config){
        this.config = config;
        this.StartKey=config.getSrk();
        this.EndKey=config.getEnk();
        String tableName=getTableNameTest(config.getRootPath());
        try {
                LOG.info("Begin to init scanner for Start row key: " + StartKey + " End row key: " + EndKey + " Table name: " + tableName);
                System.out.println("Begin to init scanner for Start row key: " + StartKey + " End row key: " + EndKey + " Table name: " + tableName);
                TableScanner scanner = new TableScanner(StartKey, EndKey, tableName, false, false);
                scanners.add(scanner);
            } catch (Exception e) {
            e.printStackTrace();
            //LOG.error("Init HBaseRecordReader error! MSG: " + e.getMessage());
        }
    }
    public HBaseRecordReader(HBaseScanPOP.HBaseScanEntry config, Filter filter){
        this.config = config;
        this.StartKey=config.getSrk();
        this.EndKey=config.getEnk();
        String tableName=getTableNameTest(config.getRootPath());
        try {
            LOG.info("Begin to init scanner for Start row key: " + StartKey + " End row key: " + EndKey + " Table name: " + tableName);
            System.out.println("Begin to init scanner for Start row key: " + StartKey + " End row key: " + EndKey + " Table name: " + tableName);
            TableScanner scanner = new TableScanner(StartKey, EndKey, tableName, false, false);
            scanners.add(scanner);
        } catch (Exception e) {
            e.printStackTrace();
            //LOG.error("Init HBaseRecordReader error! MSG: " + e.getMessage());
        }
        this.filter=filter;
    }

    private String getTableNameTest(String rootPath) {
        return rootPath.replace("xadrill","-");
    }

    public String getTableName(SchemaPath rootPath) {
        return rootPath.getPath().toString().replace("xadrill", "-");
    }
    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        try {
            this.output = output;
            valueVectors = new ValueVector<?>[config.getTypes().length];
            for(int i=0;i<config.getTypes().length;i++){
            SchemaDefProtos.MajorType type=config.getTypes()[i].getMajorType();
            int batchRecordCount=BATCHRECORDCOUNT;
            valueVectors[i] = getVector(i,config.getTypes()[i].getName(),type,batchRecordCount);
            output.setNewSchema();
            }
        } catch (Exception e) {
            throw new ExecutionSetupException("Failure while setting up fields", e);
        }
        //To change body of implemented methods use File | Settings | File Templates.
    }
    private ValueVector<?> getVector(int fieldId, String name, SchemaDefProtos.MajorType type, int length) {
        //assert context != null : "Context shouldn't be null.";

        if(type.getMode() != SchemaDefProtos.DataMode.REQUIRED) throw new UnsupportedOperationException();

        MaterializedField f = MaterializedField.create(new SchemaPath(name), fieldId, 0, type);
        ValueVector<?> v;
        BufferAllocator allocator=new DirectBufferAllocator();
        //v = TypeHelper.getNewVector(f, context.getAllocator());
        v=TypeHelper.getNewVector(f,allocator);
        v.allocateNew(length);

        return v;

    }

    @Override
    public int next() {
        int recordSetSize = 0;
        //List<ByteBuf>[] byteBufLists=(List<ByteBuf>[])new Object[valueVectors.length];
        while(true){
        if(currentScannerIndex>scanners.size()-1)return recordSetSize;
        TableScanner scanner=scanners.get(currentScannerIndex);
        if (valIndex == -1) {
            if (scanner == null) {
                return 0;
            }
            try {
                hasMore = scanner.next(curRes);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            valIndex = 0;
        }
        if (valIndex > curRes.size()-1) {
            while (hasMore) {
                        /* Get result list from the same scanner and skip curRes with no element */
                curRes.clear();
                try {
                    hasMore = scanner.next(curRes);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                valIndex = 0;
                if(!hasMore)currentScannerIndex++;
                if (curRes.size() != 0) {
                    KeyValue kv = curRes.get(valIndex++);
                    boolean next=PutValuesToVectors(kv,valueVectors,recordSetSize);
                    if(!next)return recordSetSize;
                    recordSetSize++;
                    break;
                }
            }
            if(valIndex>curRes.size()-1)continue;
        }
        KeyValue kv = curRes.get(valIndex++);
        boolean next=PutValuesToVectors(kv,valueVectors,recordSetSize);
        if(!next)return recordSetSize;
        recordSetSize++;
        }
    }
    public boolean PutValuesToVectors(KeyValue kv,ValueVector<?>[] valueVectors,int recordSetSize){
        for(int i=0;i<config.getTypes().length;i++){
            String name=config.getTypes()[i].getName();
            Object result=getValFromKeyValue(kv, name);
            ValueVector<?> valueVector=valueVectors[i];
            String resultString=null;
            long resultLong=0;
            int  resultInt=0;
            byte[] resultBytes=null;
            if(name.equals("val")){
                resultLong=(long)result;
                resultBytes=Bytes.toBytes(resultLong);
            }
            else if(name.equals("uid")|| name.equals("day")){
                resultInt=(int)result;
                resultBytes=Bytes.toBytes(resultInt);
            }

            else {
                resultString=(String)result;
                resultBytes=Bytes.toBytes(resultString);
            }
            if(valueVector instanceof VarLen4){
                ByteBuf lengthbuf=valueVector.getBuffers()[0];
                int previousOffset=lengthbuf.getInt(recordSetSize);
                if(recordSetSize==0)previousOffset=0;
                int offset=previousOffset+resultBytes.length;
                if(offset>lengthbuf.capacity()*4)return false;
                ((VarLen4)valueVector).setBytes(recordSetSize,resultBytes);
                valueVector.setRecordCount(recordSetSize);
                if((recordSetSize+1)*4>lengthbuf.capacity())return false;
            }
            else if(valueVector instanceof Fixed4){
                ((Fixed4)valueVector).setInt(recordSetSize,resultInt);
                valueVector.setRecordCount(recordSetSize);
                System.out.println(((Fixed4) valueVector).getInt(recordSetSize));
                if((recordSetSize+1)*4>valueVector.capacity())return false;
            }
            else if(valueVector instanceof Fixed8){
                ((Fixed8)valueVector).setBigInt(recordSetSize,resultLong);
                valueVector.setRecordCount(recordSetSize);
                System.out.println(((Fixed8) valueVector).getBigInt(recordSetSize));
                if((recordSetSize+1)*8>valueVector.capacity())return false;
            }
        }
        return true;
    }

    public Object  getValFromKeyValue(KeyValue keyvalue,String option){
        if(option.equals("val")){
            return Bytes.toLong(keyvalue.getValue());
        }
        else{
            byte[] rk=keyvalue.getRow();
            if(option.equals("uid")){
                long uid=getUidOfLongFromDEURowKey(rk);
                return getInnerUidFromSamplingUid(uid);
            }
            else if(option.equals("event")){
                return getEventFromDEURowKey(rk);
            }
            else if(option.equals("day")){
                return Integer.parseInt(getDayFromDEURowKey(rk));
            }
        }
        return null;
    }
    public  long getUidOfLongFromDEURowKey(byte[] rowKey) {
        byte[] uid = new byte[8];
        int i = 0;
        for (; i < 3; i++) {
            uid[i] = 0;
        }

        for (int j = rowKey.length - 5; j < rowKey.length; j++) {
            uid[i++] = rowKey[j];
        }

        return Bytes.toLong(uid);
    }

    public String getDayFromDEURowKey(byte[] rowKey){
        byte[] day=new byte[8];
        for(int i=0;i<8;i++){
            day[i]=rowKey[i];
        }
        return Bytes.toString(day);
    }

    public int getInnerUidFromSamplingUid(long suid) {
        return (int) (0xffffffffl & suid);
    }

    public String getEventFromDEURowKey(byte[] rowKey) {
        byte[] eventBytes = Arrays.copyOfRange(rowKey, 8, rowKey.length - 6);
        return Bytes.toString(eventBytes);
    }



    public void setup() {
    }

    @Override
    public void cleanup() {
        for (TableScanner scanner : scanners) {
            try {
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
                //LOG.error("Error while closing Scanner " + scanner, e);
            }
        }
    }

    public String calDay(String date, int dis) throws ParseException {
        try {
            TimeZone TZ = TimeZone.getTimeZone("GMT+8");
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            Date temp = new Date(getTimestamp(date));

            java.util.Calendar ca = Calendar.getInstance(TZ);
            ca.setTime(temp);
            ca.add(Calendar.DAY_OF_MONTH, dis);
            return df.format(ca.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            //LOG.error("CalDay got exception! " + date + " " + dis);
            throw new ParseException(date + " " + dis, 0);
        }
    }

    public long getTimestamp(String date) {
        String dateString = date + " 00:00:00";
        SimpleDateFormat tdf = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        Date nowDate = null;
        try {
            nowDate = tdf.parse(dateString);
        } catch (ParseException e) {
            //LOG.error("DateManager.daydis catch Exception with params is "
                    //+ date, e);
        }
        if (nowDate != null) {
            return nowDate.getTime();
        } else {
            return -1;
        }
    }

    public int compareDate(String DATE1, String DATE2) throws ParseException{
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception e) {
            //LOG.error("Invalid date format! Date1: " + DATE1 + "\tDate2: " + DATE2, e);
            e.printStackTrace();
            throw new ParseException(DATE1 + "\t" + DATE2, 0);
        }

    }

    public String getNextEvent(String eventFilter) {
        StringBuilder endEvent = new StringBuilder(eventFilter);
        endEvent.setCharAt(eventFilter.length() - 1, (char) (endEvent.charAt(eventFilter.length() - 1) + 1));
        return endEvent.toString();
    }

}
