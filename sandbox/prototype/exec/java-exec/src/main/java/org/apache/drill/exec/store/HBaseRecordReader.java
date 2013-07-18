package org.apache.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.xingcloud.hbase.filter.XARowKeyFilter;
import com.xingcloud.mongodb.MongoDBOperation;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.*;
import org.apache.hadoop.hbase.KeyValue;
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

    private String eventPattern;
    private String pID;
    private HbaseScanPOP.HbaseScanEntry config;
    private FragmentContext context;

    private List<TableScanner> scanners = new ArrayList<TableScanner>();
    private int currentScannerIndex = 0;
    private List<KeyValue> curRes = new ArrayList<KeyValue>();
    private int valIndex = -1;
    private boolean hasMore;
    private int BATCHRECORDCOUNT = 1024;
    private ValueVector<?>[] valueVectors;
    private boolean init = false ;

    ScanType[] types = new ScanType[]{
            new ScanType("ts", SchemaDefProtos.MinorType.UINT8, SchemaDefProtos.DataMode.REQUIRED),
            new ScanType("event", SchemaDefProtos.MinorType.VARCHAR4, SchemaDefProtos.DataMode.REQUIRED),
            new ScanType("uid", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED),
            new ScanType("val", SchemaDefProtos.MinorType.UINT8, SchemaDefProtos.DataMode.REQUIRED)
    };

    public static class ScanType {
        public SchemaDefProtos.MinorType minorType;
        private String name;
        private SchemaDefProtos.DataMode mode;

        @JsonCreator
        public ScanType(@JsonProperty("name") String name, @JsonProperty("type") SchemaDefProtos.MinorType minorType,
                        @JsonProperty("mode") SchemaDefProtos.DataMode mode) {
            this.name = name;
            this.minorType = minorType;
            this.mode = mode;
        }

        @JsonProperty("type")
        public SchemaDefProtos.MinorType getMinorType() {
            return minorType;
        }

        public String getName() {
            return name;
        }

        public SchemaDefProtos.DataMode getMode() {
            return mode;
        }

        @JsonIgnore
        public SchemaDefProtos.MajorType getMajorType() {
            SchemaDefProtos.MajorType.Builder b = SchemaDefProtos.MajorType.newBuilder();
            b.setMode(mode);
            b.setMinorType(minorType);
            return b.build();
        }

    }

    public HBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry config) {
        this.context = context;
        this.config = config;

    }


    private List<String> getDayList(String startDay, String endDay) {
        List<String> dayList = new ArrayList<String>();
        try {
            for (String day = startDay; compareDate(day, endDay) <= 0; day = calDay(day, 1)) {
                dayList.add(day);
            }
        } catch (Exception e) {

        }
        return dayList;
    }


    private String getTableNameFromProject(String pID) {
        return pID + "_deu";
    }


    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        try {
            valueVectors = new ValueVector<?>[types.length];
            for (int i = 0; i < types.length; i++) {
                SchemaDefProtos.MajorType type = types[i].getMajorType();
                int batchRecordCount = BATCHRECORDCOUNT;
                valueVectors[i] = getVector(i, types[i].getName(), type, batchRecordCount);
                output.addField(i, valueVectors[i]);
                output.setNewSchema();
            }
        } catch (Exception e) {
            throw new ExecutionSetupException("Failure while setting up fields", e);
        }
    }

    private ValueVector<?> getVector(int fieldId, String name, SchemaDefProtos.MajorType type, int length) {

        if (type.getMode() != SchemaDefProtos.DataMode.REQUIRED) throw new UnsupportedOperationException();

        MaterializedField f = MaterializedField.create(new SchemaPath(name), fieldId, 0, type);
        ValueVector<?> v;
        BufferAllocator allocator;
        if (context != null) allocator = context.getAllocator();
        else allocator = new DirectBufferAllocator();
        v = TypeHelper.getNewVector(f, allocator);
        v.allocateNew(length);

        return v;

    }

    private void init() {
        //this.pID = config.getProject();

        this.pID = "sof-dsk";
        this.eventPattern = config.getEventPattern();
        String tableName = getTableNameFromProject(pID);
        boolean allEvents = true;
        String[] events = eventPattern.split("\\.");
        for (int i = 0; i < events.length; i++) {
            if (!events[i].equals("*"))
                allEvents = false;
        }
        try {
            List<String> days = getDayList(config.getStartDate(), config.getEndDate());
            Collections.sort(days);
            if (!allEvents) {
                Set<String> eventSet = MongoDBOperation.getEventSet(pID, eventPattern);
                List<String> eventList = new ArrayList<String>(eventSet);
                Collections.sort(eventList);
                for (int i = 0; i < days.size(); i++) {
                    String day = days.get(i);
                    List<String> oneDayList = new ArrayList<String>();
                    oneDayList.add(day);
                    byte[] srk = Bytes.toBytes(day + eventList.get(0));
                    byte[] enk = Bytes.toBytes(day + getNextEvent(eventList.get(eventList.size() - 1)));
                    XARowKeyFilter filter1 = new XARowKeyFilter(0, Long.MAX_VALUE, eventList, oneDayList);
                    TableScanner scanner = new TableScanner(srk, enk, tableName, filter1, false, false);
                    scanners.add(scanner);
                }
            } else {
                for (int i = 0; i < days.size(); i++) {
                    String day = days.get(i);
                    List<String> oneDayList = new ArrayList<String>();
                    oneDayList.add(day);
                    byte[] srk = Bytes.toBytes(day);
                    byte[] enk = Bytes.toBytes(calDay(day, 1));
                    //XARowKeyFilter filter1 = new XARowKeyFilter(0, Long.MAX_VALUE, eventList, oneDayList);
                    TableScanner scanner = new TableScanner(srk, enk, tableName, null, false, false);
                    scanners.add(scanner);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int next() {
        if(!init){
            init();
            init = true;
        }
        int recordSetSize = 0;
        while (true) {
            if (currentScannerIndex > scanners.size() - 1) return recordSetSize;
            TableScanner scanner = scanners.get(currentScannerIndex);
            if (valIndex == -1) {
                if (scanner == null) {
                    return 0;
                }
                try {
                    hasMore = scanner.next(curRes);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                valIndex = 0;
            }
            if (valIndex > curRes.size() - 1) {
                if (!hasMore) {
                    currentScannerIndex++;
                    valIndex = -1;
                    continue;
                }
                while (hasMore) {
                        /* Get result list from the same scanner and skip curRes with no element */
                    curRes.clear();
                    try {
                        hasMore = scanner.next(curRes);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    valIndex = 0;
                    if (!hasMore) currentScannerIndex++;
                    if (curRes.size() != 0) {
                        KeyValue kv = curRes.get(valIndex++);
                        boolean next = PutValuesToVectors(kv, valueVectors, recordSetSize);
                        if (!next) return recordSetSize;
                        recordSetSize++;
                        break;
                    }
                }
                if (valIndex > curRes.size() - 1) {
                    if (!hasMore) valIndex = -1;
                    continue;
                }

            }
            KeyValue kv = curRes.get(valIndex++);
            boolean next = PutValuesToVectors(kv, valueVectors, recordSetSize);
            if (!next) return recordSetSize;
            recordSetSize++;
        }
    }

    public boolean PutValuesToVectors(KeyValue kv, ValueVector<?>[] valueVectors, int recordSetSize) {
        for (int i = 0; i < types.length; i++) {
            String name = types[i].getName();
            Object result = getValFromKeyValue(kv, name);
            ValueVector<?> valueVector = valueVectors[i];
            String resultString = null;
            long resultLong = 0;
            int resultInt = 0;
            byte[] resultBytes = null;
            if (name.equals("val")||name.equals("ts")) {
                resultLong = (long) result;
                resultBytes = Bytes.toBytes(resultLong);
            } else if (name.equals("uid")) {
                resultInt = (int) result;
                resultBytes = Bytes.toBytes(resultInt);
            } else {
                resultString = (String) result;
                resultBytes = Bytes.toBytes(resultString);
            }
            if (valueVector instanceof VarLen4) {
                Fixed4 lengthVector = ((VarLen4) valueVector).getLengthVector();
                int preOffset = 0;
                if (recordSetSize != 0) preOffset = lengthVector.getInt(recordSetSize - 1);
                int offset = preOffset + resultBytes.length;
                if (offset > lengthVector.capacity() * 4) return false;
                ((VarLen4) valueVector).setBytes(recordSetSize, resultBytes);
                valueVector.setRecordCount(recordSetSize);
                if (recordSetSize + 1 > valueVector.capacity()) return false;
            } else if (valueVector instanceof Fixed4) {
                ((Fixed4) valueVector).setInt(recordSetSize, resultInt);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 1) > valueVector.capacity()) return false;
            } else if (valueVector instanceof Fixed8) {
                ((Fixed8) valueVector).setBigInt(recordSetSize, resultLong);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 1) > valueVector.capacity()) return false;
            }
        }
        return true;
    }

    public Object getValFromKeyValue(KeyValue keyvalue, String option) {
        if (option.equals("val")) {
            return Bytes.toLong(keyvalue.getValue());
        } else {
            byte[] rk = keyvalue.getRow();
            if (option.equals("uid")) {
                long uid = getUidOfLongFromDEURowKey(rk);
                return getInnerUidFromSamplingUid(uid);
            } else if (option.equals("event")) {
                return getEventFromDEURowKey(rk);
            } else if (option.equals("ts")) {
                long ts=keyvalue.getTimestamp();
                return ts;
            }
        }
        return null;
    }

    public long getUidOfLongFromDEURowKey(byte[] rowKey) {
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

    public String getDayFromDEURowKey(byte[] rowKey) {
        byte[] day = new byte[8];
        for (int i = 0; i < 8; i++) {
            day[i] = rowKey[i];
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
        }
        if (nowDate != null) {
            return nowDate.getTime();
        } else {
            return -1;
        }
    }

    public int compareDate(String DATE1, String DATE2) throws ParseException {
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
