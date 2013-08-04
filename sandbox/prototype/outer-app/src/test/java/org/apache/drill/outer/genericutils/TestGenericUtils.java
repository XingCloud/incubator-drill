package org.apache.drill.outer.genericutils;

import org.apache.drill.outer.utils.GenericUtils;
import org.junit.Test;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 下午4:22
 * Package: org.apache.drill.outer.genericutils
 */
public class TestGenericUtils {

  @Test
  public void testConvertUserPropertyName2Short() throws Exception {
    short id = GenericUtils.propertyString2TinyInt("sof-dsk", "register_time");
    System.out.println(id);
  }
}
