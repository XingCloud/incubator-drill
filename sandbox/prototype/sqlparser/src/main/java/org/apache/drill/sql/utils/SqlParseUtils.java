package org.apache.drill.sql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.ref.rse.ConsoleRSE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-7-4 Time: 下午5:41 Package: org.apache.drill.utils
 */
public class SqlParseUtils {
  public static boolean isSelectStatement(Statement statement) {
    return statement instanceof Select;
  }

  public static boolean isNotSelectStatement(Statement statement) {
    return !(statement instanceof Select);
  }

  public static String sqlQuotaRemover(String s) {
    return s.replace("`", "");
  }

  public static Map<String, StorageEngineConfig> getStorageEngineMap() throws IOException {
    Map<String, StorageEngineConfig> storageEngines = new HashMap<>(3);
    ObjectMapper mapper = new ObjectMapper();
    storageEngines.put("console", mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),
                                                   ConsoleRSE.ConsoleRSEConfig.class));
    return storageEngines;
  }
}
