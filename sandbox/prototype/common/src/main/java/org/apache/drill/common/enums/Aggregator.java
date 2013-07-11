package org.apache.drill.common.enums;

/**
 * User: Z J Wu Date: 13-7-10 Time: 下午7:01 Package: org.apache.drill.common.enums
 */
public enum Aggregator {
  COUNT("count"), SUM("sum"), COUNT_DISTINCT("count_distinct");

  private String keyWord;

  private Aggregator(String keyWord) {
    this.keyWord = keyWord;
  }

  public String getKeyWord() {
    return keyWord;
  }
}
