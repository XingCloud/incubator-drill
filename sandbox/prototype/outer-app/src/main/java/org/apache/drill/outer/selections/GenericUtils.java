package org.apache.drill.outer.selections;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

/**
 * User: Z J Wu
 * Date: 13-7-30
 * Time: 上午10:01
 * Package: org.apache.drill.outer.selections
 */
public class GenericUtils {

  public static PlanProperties buildPlanProperties(String projectId) {
    PlanProperties pp = new PlanProperties();
    pp.generator = new PlanProperties.Generator();
    pp.generator.info = projectId.trim();
    pp.generator.type = "AUTO";
    pp.version = 1;
    pp.type = PlanProperties.PlanType.APACHE_DRILL_LOGICAL;
    return pp;
  }

  public static short propertyString2TinyInt(String propertyName) {
    return 64;
  }

  public static void addAll(Collection collection, byte[] bytes) {
    for (int i = 0, size = bytes.length; i < size; i++) {
      collection.add(bytes[i]);
    }
  }

  public static byte[] toBytes(Object o, String type) {
    if (o instanceof Integer) {

    }
    return null;
  }

  public static byte[] toBytes(String s) {
    if (s != null) {
      return s.getBytes();
    }
    return null;
  }

  public static byte[] toBytes(short s) {
    byte[] b = new byte[2];
    b[1] = (byte) (s & 0xff);
    b[0] = (byte) ((s >> 8) & 0xff);
    return b;
  }

  public static byte[] toBytes(int num) {
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      b[i] = (byte) (num >>> (24 - i * 8));
    }
    return b;
  }

  public static byte[] toBytes(long num) {
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (num >>> (56 - i * 8));
    }
    return b;
  }


  public static void main(String[] args) throws IOException {
    short s = 999;
    byte[] bytes = toBytes(s);
    String ss = new String(bytes, Charset.forName("utf8"));
    System.out.println(ss);
    System.out.println(Arrays.toString(bytes));
    System.out.println(new String(ss.getBytes()));

    DrillConfig config = DrillConfig.create();
    String byteBase64String = config.getMapper().writeValueAsString(bytes);
    System.out.println(byteBase64String);

    byte[] rowkeyBytes = config.getMapper().readValue(byteBase64String, byte[].class);
    System.out.println(Arrays.toString(rowkeyBytes));
  }


}
