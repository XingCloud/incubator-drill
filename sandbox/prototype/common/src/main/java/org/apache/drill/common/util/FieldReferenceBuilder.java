package org.apache.drill.common.util;

import org.apache.drill.common.expression.FieldReference;

/**
 * User: Z J Wu Date: 13-7-18 Time: 下午3:50 Package: org.apache.drill.common.util
 */
public class FieldReferenceBuilder {
  private static final char DELIMITER = '.';

  public static FieldReference buildTable(String db, String table) {
    return new FieldReference(db + DELIMITER + table);
  }

  public static FieldReference buildTable(String table) {
    return new FieldReference(table);
  }

  public static FieldReference buildColumn(String column) {
    return new FieldReference(column);
  }

  public static FieldReference buildColumn(String table, String column) {
    return new FieldReference(table + DELIMITER + column);
  }

}
