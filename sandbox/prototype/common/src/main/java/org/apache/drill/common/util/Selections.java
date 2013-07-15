package org.apache.drill.common.util;

import static org.apache.drill.common.util.DrillConstants.HBASE_TABLE_PREFIX_EVENT;
import static org.apache.drill.common.util.DrillConstants.HBASE_TABLE_PREFIX_USER;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.enums.TableType;

import java.io.IOException;

/**
 * User: Z J Wu Date: 13-7-5 Time: 下午2:05 Package: org.apache.drill.sql.utils
 */
public class Selections {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static JSONOptions NONE_HBASE_SELECTION;

  static {
    String s = String.format(new String("{\"hbase\":\"None\"}"));
    try {
      NONE_HBASE_SELECTION = MAPPER.readValue(s.getBytes(), JSONOptions.class);
    } catch (IOException e) {
      NONE_HBASE_SELECTION = null;
      e.printStackTrace();
    }
  }

  public static JSONOptions getNoneSelection() {
    return NONE_HBASE_SELECTION;
  }

  public static JSONOptions getCorrespondingSelection(String projectId, TableType tableType) throws IOException {
    String s;
    switch (tableType) {
      case EVENT:
        s = String
          .format(new String("{\"type\":\"hbase\",\"table\":\"" + projectId + HBASE_TABLE_PREFIX_EVENT + "\"}"));
        break;
      case USER:
        s = String.format(new String("{\"type\":\"hbase\",\"table\":\"" + projectId + HBASE_TABLE_PREFIX_USER + "\"}"));
        break;
      default:
        throw new IllegalArgumentException("Not supported table type - " + tableType);
    }
    return MAPPER.readValue(s.getBytes(), JSONOptions.class);
  }
}
