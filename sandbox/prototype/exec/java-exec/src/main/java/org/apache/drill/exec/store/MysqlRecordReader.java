package org.apache.drill.exec.store;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MysqlScanPOP.MysqlReadEntry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/5/13
 * Time: 11:46 AM
 */
public class MysqlRecordReader implements RecordReader {

  static ComboPooledDataSource cpds = null;
  static Logger logger = LoggerFactory.getLogger(MysqlRecordReader.class);

  private FragmentContext context ;
  private MysqlReadEntry config ;
  private String sql ;
  private Connection conn = null;
  private Statement stmt = null ;
  private ResultSet rs = null;
  private ResultSetMetaData rsMetaData = null;
  private ValueVector[] valueVectors ;


  public static Connection getConnection() throws Exception {
    if (cpds == null) {
      System.setProperty("com.mchange.v2.c3p0.cfg.xml",FileUtils.class.getResource("/mysql-c3p0.xml").getPath());
      cpds = new ComboPooledDataSource() ;
    }
    return cpds.getConnection();
  }

  public MysqlRecordReader(FragmentContext context, MysqlReadEntry config) {
    this.context = context;
    this.config = config;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {

  }

  @Override
  public int next() {
    return 0;
  }

  private void init(){
    try{
      conn = getConnection() ;
    }catch (Exception e){
        logger.error("Get mysql connection failed : " + e.getMessage());
    }

  }

  private void executeQuery() throws  ClassNotFoundException,SQLException{
    stmt = conn.createStatement();
    rs = stmt.executeQuery(sql);
    rsMetaData = rs.getMetaData();
  }

  private int HashUid2InnerUid(long suid) {
    return (int) (0xffffffffl & suid);
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
}
