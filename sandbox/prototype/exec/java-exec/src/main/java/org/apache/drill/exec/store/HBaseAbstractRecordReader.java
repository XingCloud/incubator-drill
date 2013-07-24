package org.apache.drill.exec.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseAbstractScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/23/13
 * Time: 12:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseAbstractRecordReader implements RecordReader {
    static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

    private HbaseAbstractScanPOP.HbaseAbstractScanEntry config;
    private FragmentContext context;
    private byte[] startRowKey;
    private byte[] endRowKey;
    private String tableName;

    private List<HBaseFieldInfo> projections;
    private List<LogicalExpression> filters;
    private List<HBaseFieldInfo> primaryRowKey;
    private List<KeyPart> primaryRowKeyParts;
    private Map<String,HBaseFieldInfo> fieldInfoMap;
    private Map<String, Object> rkObjectMap;
    //private boolean optional=false;
    private int index=0;

    private List<TableScanner> scanners = new ArrayList<>();
    private int currentScannerIndex = 0;
    private List<KeyValue> curRes = new ArrayList<KeyValue>();
    private int valIndex = -1;
    private boolean hasMore;
    private int BATCHRECORDCOUNT = 1024*4;
    private ValueVector<?>[] valueVectors;
    private boolean init = false ;




    public HBaseAbstractRecordReader(FragmentContext context, HbaseAbstractScanPOP.HbaseAbstractScanEntry config) {
        this.context = context;
        this.config = config;
        initConfig();
    }

    private void initConfig(){
        startRowKey=config.getStartRowKey();
        endRowKey=config.getEndRowKey();
        tableName=config.getTableName();
        projections=new ArrayList<>();
        fieldInfoMap=new HashMap<>();
        try {
            List<HBaseFieldInfo> cols= TableInfo.getCols(tableName);
            for(HBaseFieldInfo col: cols){
                fieldInfoMap.put(col.fieldSchema.getName(),col);
            }
            List<NamedExpression> logProjection=config.getProjections();
            for(NamedExpression e: logProjection){
                String name=(String)e.getRef().getPath();
                if(!fieldInfoMap.containsKey(name)){
                    LOG.debug("wrong field "+name+" hbase table has no this field");
                }else{
                    projections.add(fieldInfoMap.get(name));
                }
            }
            filters=config.getFilters();

            primaryRowKeyParts=TableInfo.getRowKey(tableName);
            primaryRowKey=new ArrayList<>();
            for(KeyPart kp: primaryRowKeyParts){
                if(kp.getType()== KeyPart.Type.field)
                    primaryRowKey.add(fieldInfoMap.get(kp.getField().getName()));
            }
            //primaryRowKey=TableInfo.getPrimaryKey(tableName);

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    private void initTableScanner() {

        scanners=new ArrayList<>();
        List<Filter> filtersList=new ArrayList<>();
        long startVersion=Long.MIN_VALUE;
        long stopVersion=Long.MAX_VALUE;
        if(filters!=null){
            for(LogicalExpression e: filters){
                if(e instanceof FunctionCall){
                    FunctionCall c=(FunctionCall)e;
                    Iterator iter=((FunctionCall) e).iterator();
                    SchemaPath leftField=(SchemaPath)iter.next();
                    ValueExpressions.LongExpression rightField=(ValueExpressions.LongExpression)iter.next();
                    HBaseFieldInfo info=fieldInfoMap.get(leftField.getPath());
                    CompareFilter.CompareOp op=CompareFilter.CompareOp.GREATER;
                    switch (c.getDefinition().getName()){
                        case "greater than":
                            op=CompareFilter.CompareOp.GREATER;
                            break;
                        case "less than":
                            op=CompareFilter.CompareOp.LESS;
                            break;
                        case "equal":
                            op=CompareFilter.CompareOp.EQUAL;
                            break;
                        case "greater than or equal to":
                            op=CompareFilter.CompareOp.GREATER_OR_EQUAL;
                            break;
                        case "less than or equal to":
                            op=CompareFilter.CompareOp.LESS_OR_EQUAL;
                            break;
                    }
                    switch (info.fieldType){
                        case cellvalue:
                            String cfName=info.cfName;
                            String cqName=info.cqName;
                            SingleColumnValueFilter valueFilter=new SingleColumnValueFilter(
                                    Bytes.toBytes(cfName),
                                    Bytes.toBytes(cqName),
                                    op,
                                    new   BinaryComparator(Bytes.toBytes(rightField.getLong()))
                            );
                            filtersList.add(valueFilter);
                            break;
                        case cversion:
                            switch (op){
                                case GREATER:
                                    startVersion=rightField.getLong()+1;
                                    break;
                                case GREATER_OR_EQUAL:
                                    startVersion=rightField.getLong();
                                    break;
                                case LESS:
                                    stopVersion=rightField.getLong();
                                    break;
                                case LESS_OR_EQUAL:
                                    stopVersion=rightField.getLong()+1;
                                    break;
                                case EQUAL:
                                    List<Long> timestamps=new ArrayList<>();
                                    timestamps.add(rightField.getLong());
                                    Filter timeStampsFilter=new TimestampsFilter(timestamps);
                                    filtersList.add(timeStampsFilter);
                                    break;
                            }
                            break;
                        case cqname:
                            Filter qualifierFilter=
                                    new QualifierFilter(op,new BinaryComparator(Bytes.toBytes(rightField.getLong())));
                            filtersList.add(qualifierFilter);
                        default:
                            break;
                    }

                }
            }
        }
        TableScanner scanner;
        if(startVersion==Long.MIN_VALUE&&stopVersion==Long.MAX_VALUE)
            scanner=new TableScanner(startRowKey,endRowKey,tableName,filtersList,false,false);
        else
            scanner=new TableScanner(startRowKey,endRowKey,tableName,filtersList,false,false,startVersion,stopVersion);
        scanners.add(scanner);
    }


    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        try {
            valueVectors = new ValueVector<?>[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                SchemaDefProtos.MajorType type=getMajorType(projections.get(i));
                int batchRecordCount = BATCHRECORDCOUNT;
                valueVectors[i] =
                        getVector(i, projections.get(i).fieldSchema.getName(), type, batchRecordCount);
                output.addField(i, valueVectors[i]);
                output.setNewSchema();
            }
        } catch (Exception e) {
            throw new ExecutionSetupException("Failure while setting up fields", e);
        }
    }

    private SchemaDefProtos.MajorType getMajorType(HBaseFieldInfo info){
        String type=info.fieldSchema.getType();
        ScanType scanType;
        switch (type){
            case "int":
                   scanType=new ScanType(info.fieldSchema.getName(), SchemaDefProtos.MinorType.INT,
                                         SchemaDefProtos.DataMode.REQUIRED);
                  return scanType.getMajorType();
            case "tinyint":
                   scanType=new ScanType(info.fieldSchema.getName(), SchemaDefProtos.MinorType.UINT1,
                                         SchemaDefProtos.DataMode.REQUIRED);
                  return scanType.getMajorType();
            case "string":
                  scanType=new ScanType(info.fieldSchema.getName(), SchemaDefProtos.MinorType.VARCHAR4,
                                        SchemaDefProtos.DataMode.REQUIRED);
                  return scanType.getMajorType();
            case "bigint":
                scanType=new ScanType(info.fieldSchema.getName(), SchemaDefProtos.MinorType.BIGINT,
                                      SchemaDefProtos.DataMode.REQUIRED);
                return scanType.getMajorType();
        }
        return null;
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



    @Override
    public int next() {
        if(!init){
            initTableScanner();
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
        rkObjectMap=new HashMap<>();
        index=0;
        parseRowKey(kv,rkObjectMap);
        for(int i=0;i< projections.size();i++){
            HBaseFieldInfo info=projections.get(i);
            ValueVector<?> valueVector = valueVectors[i];
            Object result= getValFromKeyValue(kv,info);
            String type=info.fieldSchema.getType();
            byte[] resultBytes=null;
            if(type.equals("string"))
                   resultBytes=Bytes.toBytes((String)result);
            if (valueVector instanceof VarLen4) {
                Fixed4 lengthVector = ((VarLen4) valueVector).getLengthVector();
                int preOffset = 0;
                if (recordSetSize != 0) preOffset = lengthVector.getInt(recordSetSize - 1);
                int offset = preOffset + resultBytes.length;
                if (offset > (lengthVector.capacity()+1) * 4) return false;
                ((VarLen4) valueVector).setBytes(recordSetSize, resultBytes);
                valueVector.setRecordCount(recordSetSize);
                if (recordSetSize + 2 > valueVector.capacity()) return false;
            }else if(valueVector instanceof Fixed2){
                ((Fixed2) valueVector).setUInt2(recordSetSize, (short)result);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 2) > valueVector.capacity()) return false;
            }else if (valueVector instanceof Fixed4) {
                ((Fixed4) valueVector).setInt(recordSetSize, (int)result);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 2) > valueVector.capacity()) return false;
            } else if (valueVector instanceof Fixed8) {
                ((Fixed8) valueVector).setBigInt(recordSetSize, (long)result);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 2) > valueVector.capacity()) return false;
            }else if(valueVector instanceof Fixed1){
                ((Fixed1) valueVector).setByte(recordSetSize, (byte) result);
                valueVector.setRecordCount(recordSetSize);
                if ((recordSetSize + 2) > valueVector.capacity()) return false;
            }
        }
        return true;
    }

    public Object getValFromKeyValue(KeyValue keyvalue, HBaseFieldInfo option) {
        String fieldName=option.fieldSchema.getName();
        if(option.fieldType== HBaseFieldInfo.FieldType.rowkey){
           if(!rkObjectMap.containsKey(fieldName))
               LOG.info("error! "+fieldName+" does not exists in this keyvalue");
           else
               return rkObjectMap.get(fieldName);
        }else if(option.fieldType== HBaseFieldInfo.FieldType.cellvalue){
            String cfName=Bytes.toString(keyvalue.getFamily());
            String cqName=Bytes.toString(keyvalue.getQualifier());
            if(!option.cfName.equals(cfName)|| !option.cqName.equals(cqName))
                LOG.info("error! this field's column info---"+option.cqName+":"+option.cqName+
                         " does not match the keyvalue's column info---"+cfName+":"+cqName);
            else{
                return parseBytes(keyvalue.getValue(), option.fieldSchema.getType());
            }
        }else if(option.fieldType==HBaseFieldInfo.FieldType.cversion){
            return keyvalue.getTimestamp();
        }else if(option.fieldType== HBaseFieldInfo.FieldType.cqname){
            return parseBytes(keyvalue.getQualifier(),option.fieldSchema.getType());
        }
        return null;
    }

    public void parseRowKey(KeyValue keyValue,Map<String,Object> rkObjectMap){
        byte[] rk=keyValue.getRow();
        //int index=0;
        parseRkey(rk,false,primaryRowKeyParts,null,rkObjectMap);
    }

    private void parseRkey(byte[] rk, boolean optional,List<KeyPart> keyParts,KeyPart endKeyPart,
                           Map<String,Object> rkObjectMap){
        int fieldEndindex=index;
        for(int i=0;i<keyParts.size();i++){
            KeyPart kp=keyParts.get(i);
            if(kp.getType()== KeyPart.Type.field){
                HBaseFieldInfo info=fieldInfoMap.get(kp.getField().getName());
                if(info.serType== HBaseFieldInfo.DataSerType.TEXT
                        && info.serLength!=0){
                    fieldEndindex=index+info.serLength;
                    if(optional&&fieldEndindex>rk.length)return;
                    byte[] result=Arrays.copyOfRange(rk,index,fieldEndindex);
                    String ret=Bytes.toString(result);
                    switch (info.fieldSchema.getType()){
                        case "int":
                            rkObjectMap.put(info.fieldSchema.getName(), Integer.parseInt(ret));
                            break;
                        case "tinyint":
                            rkObjectMap.put(info.fieldSchema.getName(), ret.charAt(0));
                            break;
                        case "string":
                            rkObjectMap.put(info.fieldSchema.getName(), ret);
                            break;
                        case "bigint":
                            rkObjectMap.put(info.fieldSchema.getName(), Long.parseLong(ret));
                    }
                    index=fieldEndindex;
                }
                else if(info.serType== HBaseFieldInfo.DataSerType.WORD){
                    if(i<keyParts.size()-1){
                        KeyPart nextkp=keyParts.get(i + 1);
                        String nextCons=nextkp.getConstant();
                        byte[] nextConsBytes=Bytes.toBytes(nextCons);

                        if(optional){
                            byte[] endCons=Bytes.toBytes(endKeyPart.getConstant());
                            if(endKeyPart.getConstant().equals("\\xFF"))endCons[0]=-1;
                            while(fieldEndindex<rk.length&&rk[fieldEndindex]!=nextConsBytes[0]&&
                                rk[fieldEndindex]!=endCons[0]){
                                fieldEndindex++;
                            }
                        }
                        else
                            while(fieldEndindex<rk.length&&rk[fieldEndindex]!=nextConsBytes[0]){
                                fieldEndindex++;
                            }
                    }else{
                        if(endKeyPart==null)
                            fieldEndindex=rk.length;
                        else{
                            byte[] endCons=Bytes.toBytes(endKeyPart.getConstant());
                            while(fieldEndindex<rk.length&&rk[fieldEndindex]!=endCons[0]){
                                fieldEndindex++;
                            }
                        }
                    }
                        if(fieldEndindex!=index){
                            byte[] result=Arrays.copyOfRange(rk,index,fieldEndindex);
                            String ret=Bytes.toString(result);
                            rkObjectMap.put(info.fieldSchema.getName(), ret);
                            index=fieldEndindex;
                        }else{
                            return ;
                        }

                    }else if(info.serType== HBaseFieldInfo.DataSerType.BINARY && info.serLength!=0){
                        fieldEndindex=index+info.serLength;
                        if(optional&&fieldEndindex>rk.length)return;
                        byte[] result;
                        result=Arrays.copyOfRange(rk,index,fieldEndindex);
                        Object ob=parseBytes(result,info.fieldSchema.getType());
                        rkObjectMap.put(info.fieldSchema.getName(), ob );
                }
            }else if(kp.getType()== KeyPart.Type.optionalgroup){
                List<KeyPart> optionalKeyParts=kp.getOptionalGroup();
                KeyPart endKp;
                if(optional==false) endKp=keyParts.get(i+1);
                else endKp=endKeyPart;
                parseRkey(rk,true,optionalKeyParts,endKp,rkObjectMap);

            }else if(kp.getType()==KeyPart.Type.constant){
                index++;
            }
        }
    }
    private Object parseBytes(byte[] orig,String type){
        byte[] result;
        int index=0;
        switch (type){
            case "int":
                result=new byte[4];
                for(int i=0;i<4-orig.length;i++)
                    result[i]=0;
                for(int i=4-orig.length;i<4;i++)
                    result[i]=orig[index++];
                return Bytes.toInt(result);
            case "tinyint":
                return orig[0];
            case "string":
                return Bytes.toString(orig);
            case "bigint":
                result=new byte[8];
                for(int i=0;i<8-orig.length;i++)
                    result[i]=0;
                for(int i=8-orig.length;i<8;i++)
                    result[i]=orig[index++];
                return Bytes.toLong(result);
    }
        return null;
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


}

