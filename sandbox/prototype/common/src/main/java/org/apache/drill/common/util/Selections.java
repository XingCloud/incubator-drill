package org.apache.drill.common.util;

import static org.apache.drill.common.util.DrillConstants.HBASE_TABLE_PREFIX_USER;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;

import java.io.IOException;

/**
 * User: Z J Wu Date: 13-7-5 Time: 下午2:05 Package: org.apache.drill.sql.utils
 */
public class Selections {
  public static final String SELECTION_KEY_WORD_TABLE = "table";
  public static final String SELECTION_KEY_WORD_ROWKEY = "rowkey";
  public static final String SELECTION_KEY_WORD_ROWKEY_START = "start";
  public static final String SELECTION_KEY_WORD_ROWKEY_END = "end";
  public static final String SELECTION_KEY_WORD_ROWKEY_INCLUDES = "includes";
  public static final String SELECTION_KEY_WORD_FILTERS = "filters";
  public static final String SELECTION_KEY_WORD_FILTER_TYPE = "type";
  public static final String SELECTION_KEY_WORD_FILTER = "filter";
  public static final String SELECTION_KEY_WORD_PROJECTIONS = "projections";

  public static final String SELECTION_KEY_WORD_B_DATE = "start_date";
  public static final String SELECTION_KEY_WORD_E_DATE = "end_date";
  public static final String SELECTION_KEY_WORD_EVENT = "event";
  public static final String SELECTION_KEY_WORD_PROPERTY = "prop";
  public static final String SELECTION_KEY_WORD_PROPERTY_VALUE = "propv";

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

  public static JSONOptions buildEventSelection(String projectId, String realBeginDate, String realEndDate,
                                                String event) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"");
    sb.append(SELECTION_KEY_WORD_TABLE);
    sb.append("\":\"");
    sb.append(projectId);
    sb.append("_deu\",\"");
    sb.append(SELECTION_KEY_WORD_B_DATE);
    sb.append("\":\"");
    sb.append(realBeginDate);
    sb.append("\",\"");
    sb.append(SELECTION_KEY_WORD_E_DATE);
    sb.append("\":\"");
    sb.append(realEndDate);
    sb.append("\",\"");
    sb.append(SELECTION_KEY_WORD_EVENT);
    sb.append("\":\"");
    sb.append(event);
    sb.append("\"}");
    return MAPPER.readValue(sb.toString().getBytes(), JSONOptions.class);
  }

  public static JSONOptions buildUserSelection(String projectId, String property) throws IOException {
    String s = "{\"table\":\"" + projectId + HBASE_TABLE_PREFIX_USER + "\",\"" + SELECTION_KEY_WORD_PROPERTY + "\":\"" + property + "\"}";
    return MAPPER.readValue(s.getBytes(), JSONOptions.class);
  }

  public static JSONOptions buildUserSelection(String projectId, String property, String propertyValue) throws
    IOException {
    String s = "{\"table\":\"" + projectId +
      HBASE_TABLE_PREFIX_USER + "\",\"" + SELECTION_KEY_WORD_PROPERTY +
      "\":\"" + property + "\",\"" + SELECTION_KEY_WORD_PROPERTY_VALUE + "\":\"" + propertyValue + "\"}";
    return MAPPER.readValue(s.getBytes(), JSONOptions.class);
  }

}
