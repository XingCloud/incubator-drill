package org.apache.drill.exec.store;

import com.google.common.collect.Maps;
import com.xingcloud.mysql.MySqlResourceManager;
import com.xingcloud.mysql.UserProp;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MysqlScanPOP.MysqlReadEntry;
import org.apache.drill.exec.physical.config.UserScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.logicalplan.LogicalPlanUtil;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.HBaseClientScanner;
import org.apache.hadoop.hbase.regionserver.XAScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: liqiang
 */
public class UserRecordReader implements RecordReader {

    static Logger logger = LoggerFactory.getLogger(UserRecordReader.class);
    private FragmentContext context;
    private UserScanPOP.UserReadEntry config;
    private String sql;
    private ValueVector[] valueVectors;
    private OutputMutator output;
    private Map<String, UserProp> propMap = Maps.newHashMap();
    private List<Pair<String, String>> projections;
    private String project;
    private static String cfName = "v";
    private static String cqName = "v";
    private byte[] startRowKey;
    private byte[] endRowKey;
    private FilterList filterList = new FilterList();
    private XAScanner scanner;
    private String tableName = "user_attribute";
    private List<KeyValue> curRes = new ArrayList<>();
    private int valIndex = 0;
    private int batchSize = 1024 * 64;
    private long scanCost = 0;
    private long parseCost = 0;
    private long setVectorCost = 0;
    boolean hasMore = true;
    int totalCount = 0;
    private UserProp userProp;
//    private BufferAllocator allocator;


    private static PropManager propManager = new PropManager();

    public UserRecordReader(FragmentContext context, UserScanPOP.UserReadEntry config) {
        this.context = context;
        this.config = config;
        //for test
//        allocator = BufferAllocator.getAllocator(DrillConfig.create());
    }


    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        this.output = output;
        try {
            initConfig();
            valueVectors = new ValueVector[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                MajorType type = getMajorType(projections.get(i).getFirst());
                String field = projections.get(i).getFirst();
                if (field.equals("uid")) {
                    type = Types.required(MinorType.INT);
                }
                valueVectors[i] = getVector(field, type);
                output.addField(valueVectors[i]);
                output.setNewSchema();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Mysql record reader setup failed : " + e.getMessage());
            throw new ExecutionSetupException("Failure while setting up fields", e);
        }
    }

    private MajorType getMajorType(String propertyName) throws SQLException {
        if ("uid".equals(propertyName)) {
            return Types.required(MinorType.INT);
        }
        UserProp userProp = propMap.get(propertyName);
        // if propertyName not exist ,update cache
        if (userProp == null) {
            propMap = propManager.update(project);
            userProp = propMap.get(propertyName);
        }
        if (userProp != null) {
            switch (userProp.getPropType()) {
                case sql_bigint:
                case sql_datetime:
                    return Types.required(MinorType.BIGINT);
                case sql_string:
                    return Types.required(MinorType.VARCHAR);
            }
        }
        logger.error("PropType not found for " + propertyName);
        return null;
    }

    private ValueVector getVector(String field, MajorType type) {
        if (type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();
        MaterializedField f = MaterializedField.create(new SchemaPath(field, ExpressionPosition.UNKNOWN), type);

        return TypeHelper.getNewVector(f, context.getAllocator());
        //for test
//        return TypeHelper.getNewVector(f, allocator);
    }

    @Override
    public int next() {
        if (scanner == null) {
            try {
                initScanner();
            } catch (Exception e) {
                e.printStackTrace();
                throw new DrillRuntimeException("Init mysql connection failed : " + e.getMessage());
            }
        }
        for (ValueVector v : valueVectors) {
            AllocationHelper.allocate(v, batchSize, 8);
        }

        int recordSetIndex = 0;
        while (true) {
            if (valIndex < curRes.size()) {
                int length = Math.min(batchSize - recordSetIndex, curRes.size() - valIndex);
                setValues(curRes, valIndex, length, recordSetIndex);
                recordSetIndex += length;
                if (valIndex + length != curRes.size()) {
                    valIndex += length;
                    return endNext(recordSetIndex);
                } else {
                    valIndex = 0;
                    curRes.clear();
                }
            }
            try {
                if (hasMore) {
                    long scannerStart = System.currentTimeMillis();
                    hasMore = scanner.next(curRes);
                    scanCost += System.currentTimeMillis() - scannerStart;
                }
                if (curRes.size() == 0) {
                    return endNext(recordSetIndex);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new DrillRuntimeException("Scanner failed .", e);
            }
        }
    }

    private void setValueCount(int valueCount) {
        for (int i = 0; i < valueVectors.length; i++) {
            valueVectors[i].getMutator().setValueCount(valueCount);
        }
    }

    private void setValues(List<KeyValue> keyValues, int offset, int length, int setIndex) {
        for (int i = offset; i < offset + length; i++) {
            try {
                setValues(keyValues.get(i), setIndex);
                setIndex++;
            } catch (Exception e) {
                logger.error("Ignore this keyvalue .");
                e.printStackTrace();
            }
        }
    }

    public void setValues(KeyValue keyValue, int index) {
        long uid = Bytes.toLong(Bytes.tail(keyValue.getRow(), 8));

        Object result = null;
        switch (userProp.getPropType()) {
            case sql_bigint:
                result = Bytes.toLong(keyValue.getValue());
                break;
            case sql_datetime:
                result = Bytes.toLong(keyValue.getValue());
                break;
            case sql_string:
                result = keyValue.getValue();
        }

        for (int i = 0; i < projections.size(); i++) {
            ValueVector valueVector = valueVectors[i];

            if (projections.get(i).getSecond().equals("uid")) {
                result = getInnerUidFromSamplingUid(uid);
            }
            valueVector.getMutator().setObject(index, result);
        }
    }

    public int endNext(int valueCount) {
        totalCount += valueCount;
        if (valueCount == 0)
            return 0;
        setValueCount(valueCount);
        return valueCount;
    }

    private void initConfig() throws Exception {
        String fields[] = config.getTableName().split("\\.");
        project = fields[0];
        propMap = propManager.getUserProp(project);

        String dbName = "16_" + project;
        String tableName = fields[1];
        userProp = propMap.get(tableName);

        projections = new ArrayList<>();
        String selection = "SELECT ";
        boolean isFirst = true;
        for (NamedExpression e : config.getProjections()) {
            String ref = (String) e.getRef().getPath();
            String name = (String) ((SchemaPath) e.getExpr()).getPath();
            projections.add(new Pair<>(ref, name));
            if (isFirst) {
                isFirst = false;
            } else {
                selection += ",";
            }
            selection += name + " as " + ref;
        }
        selection += " FROM `" + dbName + "`.`" + tableName + "`";
        String filter = config.getFilter();
        if (filter != null && !filter.equals("")) {
            selection += " WHERE " + filter;
        }
        sql = selection;
        logger.info("SQL : {}", sql);

        startRowKey = Bytes.add(Bytes.toBytes(Integer.parseInt(fields[2])), Bytes.toBytes(Integer.parseInt(fields[3])));
        endRowKey = Bytes.add(Bytes.toBytes(Integer.parseInt(fields[2])), Bytes.toBytes(Integer.parseInt(fields[3]) + 1));

        //filter
        if (filter != null && !filter.equals("")) {
            LogicalExpression e = parseExpr(filter);

            List<LogicalPlanUtil.UnitFunc> funcs = LogicalPlanUtil.parseUserFunctionCall((FunctionCall) e, DrillConfig.create());
            for (LogicalPlanUtil.UnitFunc func : funcs) {
                if(!"val".equals(func.getField())) {
                    continue;
                }

                CompareFilter.CompareOp op = CompareFilter.CompareOp.GREATER;
                switch (func.getOp()) {
                    case "greater than":
                        op = CompareFilter.CompareOp.GREATER;
                        break;
                    case "less than":
                        op = CompareFilter.CompareOp.LESS;
                        break;
                    case "equal":
                        op = CompareFilter.CompareOp.EQUAL;
                        break;
                    case "greater than or equal to":
                        op = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                        break;
                    case "less than or equal to":
                        op = CompareFilter.CompareOp.LESS_OR_EQUAL;
                        break;
                }
                System.out.println(func.getField() + " " +func.getOp() + " " + op.name());

                WritableByteArrayComparable comparable = null;
                switch (userProp.getPropType()) {
                    case sql_bigint:
                        comparable = new BinaryComparator(Bytes.toBytes(Long.parseLong(func.getValue())));
                        break;
                    case sql_datetime:
                        comparable = new BinaryComparator(Bytes.toBytes(Long.parseLong(func.getValue())));
                        break;
                    default:
                        comparable = new BinaryComparator(Bytes.toBytes(func.getValue()));
                }

                SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(cfName),
                        Bytes.toBytes(cqName),
                        op,
                        comparable
                );
                filterList.addFilter(valueFilter);
            }
        }

    }


    private LogicalExpression parseExpr(String expr) throws RecognitionException {
        ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);
        parser.setRegistry(new FunctionRegistry(DrillConfig.create()));
        ExprParser.parse_return ret = parser.parse();
        return ret.e;
    }

    private void initScanner() throws Exception {
        long start = System.nanoTime();
        scanner= new HBaseClientScanner(startRowKey,endRowKey,tableName,filterList);
        StringBuilder summary = new StringBuilder(tableName +"　StartKey: " + Bytes.toStringBinary(startRowKey) +
                "\tEndKey: " + Bytes.toStringBinary(endRowKey) );

        logger.info(summary.toString());
        logger.info("Init HBaseClientMultiScanner cost {} mills .", (System.nanoTime() - start) / 1000000);
    }


    @Override
    public void cleanup() {
        logger.info("Record count for entry [tableName:{},keyRange:[{}:{}],count:{}]", config.getTableName(), Bytes.toStringBinary(startRowKey), Bytes.toStringBinary(endRowKey), totalCount);
        logger.info("HbaseRecordReader finished . ");
        for (int i = 0; i < valueVectors.length; i++) {
            try {
                output.removeField(valueVectors[i].getField());
            } catch (SchemaChangeException e) {
                logger.warn("Failure while trying to remove field.", e);
            }
            valueVectors[i].close();
        }
        try {
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
        } catch (Exception e) {
            logger.error("Scanners close failed : " + e.getMessage());
        }
        logger.debug("User scan cost {} , parse cost {} ,setVectorCost {} ", scanCost, parseCost / 1000000, (setVectorCost - parseCost) / 1000000);
    }

    private int getInnerUidFromSamplingUid(long suid) {
        return (int) (0xffffffffl & suid);
    }

static class PropManager {

    PropCache cache = new PropCache();

    public synchronized Map<String, UserProp> update(String pID) throws SQLException {
        removeUserProp(pID);
        Map<String, UserProp> userPropMap = getUserPropMap(pID);
        cache.putCache(pID, userPropMap);
        return userPropMap;
    }

    public synchronized Map<String, UserProp> getUserProp(String pID) throws SQLException {
        Map<String, UserProp> userPropMap = cache.getCache(pID);
        if (userPropMap == null) {
            userPropMap = getUserPropMap(pID);
            cache.putCache(pID, userPropMap);
        }
        return userPropMap;
    }

    private Map<String, UserProp> getUserPropMap(String pID) throws SQLException {
        long st = System.nanoTime();
        List<UserProp> userPropList = MySqlResourceManager.getInstance().getUserPropsFromLocal(pID);
        logger.info("Get user property form mysql taken " + (System.nanoTime() - st) / 1.0e9 + " sec."
                + pID + " property size: " + userPropList.size());
        Map<String, UserProp> userPropMap = Maps.newHashMap();
        for (UserProp userProp : userPropList) {
            userPropMap.put(userProp.getPropName(), userProp);
        }
        return userPropMap;
    }

    private void removeUserProp(String pID) {
        cache.removeCache(pID);
    }

    class PropCache {
        Map<String, Map<String, UserProp>> cache = Maps.newHashMap();

        public Map<String, UserProp> getCache(String pID) throws SQLException {
            return cache.get(pID);
        }

        public void putCache(String pID, Map<String, UserProp> userPropMap) {
            cache.put(pID, userPropMap);
        }

        public void removeCache(String pID) {
            if (cache.containsKey(pID))
                cache.remove(pID);
        }

    }
}

}
