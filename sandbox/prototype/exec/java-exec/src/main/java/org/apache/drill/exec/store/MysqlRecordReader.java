package org.apache.drill.exec.store;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MysqlScanPOP.MysqlReadEntry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
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

  private List<HBaseFieldInfo> projections;
  private Map<String, HBaseFieldInfo> fieldInfoMap;


  private final int batchSize = 1024;

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
    try {
      initConfig();
      initStmtExecutor();
      valueVectors = new ValueVector[projections.size()];
      for (int i = 0; i < projections.size(); i++) {
        MajorType type = getMajorType(projections.get(i));
        String field = projections.get(i).fieldSchema.getName();
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
  }

  private MajorType getMajorType(HBaseFieldInfo info) {
    String type = info.fieldSchema.getType();
    switch (type) {
      case "int":
        return Types.required(MinorType.INT);
      case "tinyint":
        return Types.required(MinorType.UINT1);
      case "string":
        return Types.required(MinorType.VARCHAR);
      case "bigint":
        return Types.required(MinorType.BIGINT);
      case "smallint":
        return Types.required(MinorType.SMALLINT);
    }
    return null;
  }

  private ValueVector getVector(String field, MajorType type) {

    if (type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();
    MaterializedField f = MaterializedField.create(new SchemaPath(field, ExpressionPosition.UNKNOWN), type);
    return TypeHelper.getNewVector(f, context.getAllocator());
  }

  @Override
  public int next() {
    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 50);
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
    }
    return 0;
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.length; i++) {
      valueVectors[i].getMutator().setValueCount(valueCount);
    }
  }

  public boolean setValues(ResultSet rs, ValueVector[] valueVectors, int index) {
    boolean next = true;
    for (int i = 0; i < projections.size(); i++) {
      HBaseFieldInfo info = projections.get(i);
      ValueVector valueVector = valueVectors[i];
      Object result = null;
      try {
        result = rs.getObject(i + 1);
      } catch (SQLException e) {
        logger.error("" + e.getMessage());
      }
      String type = info.fieldSchema.getType();
      if (type.equals("string"))
        result = Bytes.toBytes((String) result);

      // TODO
      if (projections.get(i).fieldSchema.getName().equals("uid")) {
        result = getInnerUidFromSamplingUid((Long) result);
      }

      valueVector.getMutator().setObject(index, result);
      if (valueVector.getValueCapacity() - index == 1) {
        next = false;
      }
    }

    return next;
  }

  private void initConfig() throws Exception {
    String fields[] = config.getTableName().split("\\.");
    String project = fields[0];
    String dbName = "fix_" + project;
    String tableName = fields[1];

    projections = new ArrayList<>();
    fieldInfoMap = new HashMap<>();
    List<NamedExpression> logProjection = config.getProjections();
    List<String> options = new ArrayList<>();
    for (int i = 0; i < logProjection.size(); i++) {
      options.add((String) ((SchemaPath) logProjection.get(i).getRef()).getPath());
    }

    List<HBaseFieldInfo> cols = TableInfo.getCols("mysql_property_" + project, options);
    for (HBaseFieldInfo col : cols) {
      fieldInfoMap.put(col.fieldSchema.getName(), col);
    }

    String selection = "SELECT ";
    boolean isFirst = true;

    for (NamedExpression e : logProjection) {
      String ref = (String) e.getRef().getPath();
      String name = (String) ((SchemaPath) e.getExpr()).getPath();
      if (isFirst) {
        isFirst = false;
      } else {
        selection += ",";
      }
      selection += name + " as " + ref;

      if (!fieldInfoMap.containsKey(ref)) {
        logger.debug("wrong field " + ref + " hbase table has no this field");
      } else {
        projections.add(fieldInfoMap.get(ref));
      }
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
    if (conn != null) {
      try {
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

}
