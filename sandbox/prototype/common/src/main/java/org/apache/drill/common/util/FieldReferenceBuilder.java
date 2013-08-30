package org.apache.drill.common.util;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;

/**
 * User: Z J Wu Date: 13-7-18 Time: 下午3:50 Package: org.apache.drill.common.util
 */
public class FieldReferenceBuilder {
  private static final char DELIMITER = '.';

  public static FieldReference buildTable(String db, String table) {
    return new FieldReference(db + DELIMITER + table, ExpressionPosition.UNKNOWN);
  }

  public static FieldReference buildTable(String table) {
    return new FieldReference(table,ExpressionPosition.UNKNOWN);
  }

  public static FieldReference buildColumn(String column) {
    return new FieldReference(column,ExpressionPosition.UNKNOWN);
  }

  public static FieldReference buildColumn(String table, String column) {
    return new FieldReference(table + DELIMITER + column,ExpressionPosition.UNKNOWN);
  }

}
