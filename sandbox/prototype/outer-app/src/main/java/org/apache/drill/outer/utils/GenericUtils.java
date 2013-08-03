package org.apache.drill.outer.utils;

import com.xingcloud.userprops_meta_util.UserProp;
import com.xingcloud.userprops_meta_util.UserProps_DEU_Util;
import org.apache.drill.common.PlanProperties;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Z J Wu
 * Date: 13-7-30
 * Time: 上午10:01
 * Package: org.apache.drill.outer.selections
 */
public class GenericUtils {
  private static final Map<String, Integer> USER_PROP_MAP = new ConcurrentHashMap<>(500);

  public static PlanProperties buildPlanProperties(String projectId) {
    PlanProperties pp = new PlanProperties();
    pp.generator = new PlanProperties.Generator();
    pp.generator.info = projectId.trim();
    pp.generator.type = "AUTO";
    pp.version = 1;
    pp.type = PlanProperties.PlanType.APACHE_DRILL_LOGICAL;
    return pp;
  }

  public static short propertyString2TinyInt(String projectId, String propName) throws Exception {
    String key = projectId + "." + propName;
    Integer id = USER_PROP_MAP.get(key);
    if (id != null) {
      return id.shortValue();
    }
    List<UserProp> properties = UserProps_DEU_Util.getInstance().getUserProps(projectId);
    short thisId = 0;
    String pName;
    for (UserProp up : properties) {
      pName = up.getPropName();
      if (propName.equals(pName)) {
        thisId = (short) up.getId();
      }
      USER_PROP_MAP.put(projectId + "." + up.getPropName(), up.getId());
    }
    if (thisId == 0) {
      throw new Exception("No such property(" + propName + ") in project(" + projectId + ")");
    }
    return thisId;
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

  public static void main(String[] args) throws Exception {
    propertyString2TinyInt("age", "register_time");
  }

}
