package org.apache.drill.sql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;

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
}
