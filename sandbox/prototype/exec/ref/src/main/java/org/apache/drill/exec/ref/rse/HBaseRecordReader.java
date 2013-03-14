package org.apache.drill.exec.ref.rse;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.drill.exec.ref.ReferenceInterpreter;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-11
 * Time: 下午2:06
 * To change this template use File | Settings | File Templates.
 */
public class HBaseRecordReader implements RecordReader {
    private static Logger LOG = LoggerFactory.getLogger(ReferenceInterpreter.class);

    private String startDate;
    private String endDate;
    private String l0;
    private String l1;
    private String l2;
    private String l3;
    private String l4;

    private long totalRecords = 0;
    private List<TableScanner> scanners = new ArrayList<TableScanner>();
    private int currentScannerIndex = 0;
    private List<KeyValue> curRes = new ArrayList<KeyValue>();
    private int valIndex = -1;
    private boolean hasMore;

    private ROP parent;
    private SchemaPath rootPath;
    private UnbackedRecord record = new UnbackedRecord();

    private  long wclCount = 0;


    public HBaseRecordReader(String startDate, String endDate, String l0, String l1, String l2, String l3, String l4, ROP parent, SchemaPath rootPath) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.l0 = l0;
        this.l1 = l1;
        this.l2 = l2;
        this.l3 = l3;
        this.l4 = l4;
        this.parent = parent;
        this.rootPath = rootPath;
        String event = l0 + ".*";
        String nextEvent = getNextEvent(l0) + ".*";
        String tableName = getTableName(rootPath);
        try {
            for (String date=startDate; compareDate(date, endDate)<=0; date=calDay(date, 1)) {
                String srk = date + event;
                String erk = date + nextEvent;
                LOG.info("Begin to init scanner for Start row key: " + srk + " End row key: " + erk + " Table name: " + tableName);
                System.out.println("Begin to init scanner for Start row key: " + srk + " End row key: " + erk + " Table name: " + tableName);
                TableScanner scanner = new TableScanner(srk, erk, tableName, false, false);
                scanners.add(scanner);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            LOG.error("Init HBaseRecordReader error! MSG: " + e.getMessage());
        }

    }

    private class NodeIter implements RecordIterator {


        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        @Override
        public NextOutcome next() {
            try {
                if (valIndex == -1) {
                    LOG.info("First time call next() method in HBaseRecordReader...");
                    /* First time call this method */
                    if (scanners.size() == 0) {
                        return NextOutcome.NONE_LEFT;
                    }
                    TableScanner scanner = scanners.get(currentScannerIndex);
                    hasMore = scanner.next(curRes);
                    valIndex = 0;
                }

                if (valIndex > curRes.size()-1) {

                    if (hasMore) {
                        /* Get result list from the same scanner */
                        TableScanner scanner = scanners.get(currentScannerIndex);
                        curRes.clear();
                        hasMore = scanner.next(curRes);
                        valIndex = 0;
                    } else {
                        /* Get result list from another scanner */
                        currentScannerIndex++;
                        if (currentScannerIndex > scanners.size()-1) {
                            /* Already reached the last one */
                            LOG.info("No more scanner left...");
                            return NextOutcome.NONE_LEFT;

                        } else {
                            LOG.info("Get data from next scanner...");
                            TableScanner scanner = scanners.get(currentScannerIndex);
                            curRes.clear();
                            valIndex = 0;
                            hasMore = scanner.next(curRes);

                            while (curRes.size() == 0) {
                                /* Get next scanner with actual values */
                                currentScannerIndex++;
                                if (currentScannerIndex > scanners.size()-1) {
                                    /* Already reached the last one */
                                    LOG.info("No more scanner left...");
                                    return NextOutcome.NONE_LEFT;
                                } else {
                                    LOG.info("Get data from next scanner...");
                                    scanner = scanners.get(currentScannerIndex);
                                    curRes.clear();
                                    valIndex = 0;
                                    hasMore = scanner.next(curRes);
                                }
                            }
                        }
                    }
                }
                KeyValue kv = curRes.get(valIndex++);
                record.setClearAndSetRoot(rootPath, convert(kv));
                return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
            } catch (IOException e) {
                throw new RecordException("Failure while reading record", null, e);
            }
        }

        @Override
        public ROP getParent() {
            return parent;
        }
    }

    @Override
    public RecordIterator getIterator() {
        return new NodeIter();
    }

    @Override
    public void setup() {
    }

    @Override
    public void cleanup() {
        for (TableScanner scanner : scanners) {
            try {
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Error while closing Scanner " + scanner, e);
            }
        }
    }



    public DataValue convert(KeyValue kv) {

        SimpleMapValue map = new SimpleMapValue();
        byte[] rk = kv.getRow();

        /* Set uid */
        long uid = getUidOfLongFromDEURowKey(rk);
        int innerUid = getInnerUidFromSamplingUid(uid);
        DataValue uidDV = new ScalarValues.IntegerScalar(innerUid);
        map.setByName("uid", uidDV);
        /* Set event value */
        long eventVal = Bytes.toLong(kv.getValue());
        DataValue valDV = new ScalarValues.LongScalar(eventVal);
        map.setByName("value", valDV);
        /* Set event name */
        String event = getEventFromDEURowKey(rk);
        String[] fields = event.split("\\.");
        int i = 0;
        for (;i<fields.length;i++) {
            String name = fields[i];
            DataValue nameDV = new ScalarValues.StringScalar(name);
            map.setByName("l"+i, nameDV);
        }
        for (; i<5; i++) {
            DataValue nameDV = new ScalarValues.StringScalar("*");
            map.setByName("l"+i, nameDV);
        }
        /* Set timestamp */
        long ts = kv.getTimestamp();
        DataValue tsDV = new ScalarValues.LongScalar(ts);
        map.setByName("ts", tsDV);

        LOG.info(map.toString());
        //System.out.println(innerUid+"\t"+eventVal+"\t"+ts);
        return map;
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

    public int getInnerUidFromSamplingUid(long suid) {
        return (int) (0xffffffffl & suid);
    }

    public String getEventFromDEURowKey(byte[] rowKey) {
        byte[] eventBytes = Arrays.copyOfRange(rowKey, 8, rowKey.length - 6);
        return Bytes.toString(eventBytes);
    }

    public String getTableName(SchemaPath rootPath) {
        return rootPath.getPath().toString().replace("xadrill", "-");
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
            LOG.error("CalDay got exception! " + date + " " + dis);
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
            LOG.error("DateManager.daydis catch Exception with params is "
                    + date, e);
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
            LOG.error("Invalid date format! Date1: " + DATE1 + "\tDate2: " + DATE2, e);
            e.printStackTrace();
            throw new ParseException(DATE1 + "\t" + DATE2, 0);
        }

    }

    public String getNextEvent(String eventFilter) {
        StringBuilder endEvent = new StringBuilder(eventFilter);
        endEvent.setCharAt(eventFilter.length() - 1, (char) (endEvent.charAt(eventFilter.length() - 1) + 1));
        return endEvent.toString();
    }

    public static void main(String[] args) throws Exception{
        //HBaseRecordReader hBaseRecordReader = new HBaseRecordReader();
    }
}
