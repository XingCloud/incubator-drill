package org.apache.drill.outer.enums;

/**
 * User: Z J Wu Date: 13-7-5 Time: 上午10:37 Package: org.apache.drill.sql.visitors
 */
public enum BinaryOperator {
  AND("and"), OR("OR"), GE(">="), GT(">"), LE("<="), LT("<"), EQ("=");

  private String sqlName;

  private BinaryOperator(String sqlName) {
    this.sqlName = sqlName;
  }

  public String getSqlName() {
    return sqlName;
  }
}
