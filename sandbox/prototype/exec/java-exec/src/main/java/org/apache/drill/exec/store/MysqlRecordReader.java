package org.apache.drill.exec.store;

import com.google.common.collect.Maps;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MysqlScanPOP.MysqlReadEntry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/5/13
 * Time: 11:46 AM
 */
public class MysqlRecordReader implements RecordReader {

  static ComboPooledDataSource cpds = null;
  static Logger logger = LoggerFactory.getLogger(MysqlRecordReader.class);
  private FragmentContext context;
  private MysqlReadEntry config;
  private String sql;
  private Connection conn = null;
  private Statement stmt = null;
  private ResultSet rs = null;
  private ValueVector[] valueVectors;
  private OutputMutator output;
  private Map<String, UserProp> propMap = Maps.newHashMap();
  private List<Pair<String, String>> projections;
  private String project;
  private final int batchSize = 16 * 1024;

  private static PropManager propManager = new PropManager();

  public static synchronized Connection getConnection() throws Exception {
    if (cpds == null) {
      cpds = new ComboPooledDataSource();
    }
    return cpds.getConnection();
  }


  public MysqlRecordReader(FragmentContext context, MysqlReadEntry config) {
    this.context = context;
    this.config = config;
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    long start = System.nanoTime();
    this.output = output;
    try {
      initConfig();
      //initStmtExecutor();
      valueVectors = new ValueVector[projections.size()];
      for (int i = 0; i < projections.size(); i++) {
        MajorType type = getMajorType(projections.get(i).getFirst());
        String field = projections.get(i).getFirst();
        if (field.equals("uid")) {
          type = Types.required(MinorType.INT);
        }
        valueVectors[i] =
          getVector(field, type);
        output.addField(valueVectors[i]);
        output.setNewSchema();
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Mysql record reader setup failed : " + e.getMessage());
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
    logger.info("Setup cost {} mills.", (System.nanoTime() - start) / 1000000);
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
  }

  @Override
  public int next() {
    if (conn == null) {
      try {
        initStmtExecutor();
      } catch (Exception e) {
        logger.error("Get mysql connection failed .");
        throw new DrillRuntimeException("Get mysql connection failed .");
      }
    }
    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 8);
    }
    int recordSetIndex = 0;
    try {
      while (rs.next()) {
        boolean next = setValues(rs, valueVectors, recordSetIndex);
        recordSetIndex++;
        if (!next)
          break;
      }
      setValueCount(recordSetIndex);
      return recordSetIndex;
    } catch (SQLException e) {
      logger.error("Scan mysql failed : " + e.getMessage());
      throw new DrillRuntimeException("Scan mysql failed : " + e.getMessage());
    }
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.length; i++) {
      valueVectors[i].getMutator().setValueCount(valueCount);
    }
  }

  public boolean setValues(ResultSet rs, ValueVector[] valueVectors, int index) {
    boolean next = true;
    for (int i = 0; i < projections.size(); i++) {
      ValueVector valueVector = valueVectors[i];
      Object result = null;
      try {
        result = rs.getObject(i + 1);
      } catch (SQLException e) {
        logger.error("" + e.getMessage());
        throw new DrillRuntimeException("Scan mysql failed : " + e.getMessage());
      }
      if (valueVector instanceof VarCharVector)
        result = Bytes.toBytes((String) result);
      // TODO
      if (projections.get(i).getSecond().equals("uid")) {
        result = getInnerUidFromSamplingUid((Long) result);
      }
      valueVector.getMutator().setObject(index, result);
      if (batchSize - index == 1) {
        next = false;
      }
    }
    return next;
  }

  private void initConfig() throws Exception {
    String fields[] = config.getTableName().split("\\.");
    project = fields[0];
    propMap = propManager.getUserProp(project);
    String dbName = "16_" + project;
    String tableName = fields[1];
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
  }

  private void initStmtExecutor() throws SQLException, Exception {
    conn = getConnection();
    stmt = conn.createStatement();
    rs = stmt.executeQuery(sql);
  }


  @Override
  public void cleanup() {
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying to remove field.", e);
      }
      valueVectors[i].close();
    }
    if (conn != null) {
      try {
        logger.info("Recycle connection resource . ");
        rs.close();
        stmt.close();
        conn.close();
      } catch (Exception e) {
        logger.error("Mysql connection close failed : " + e.getMessage());
      }
    }
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
      List<UserProp> userPropList = MySql_16seqid.getInstance().getUserProps(pID);
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
