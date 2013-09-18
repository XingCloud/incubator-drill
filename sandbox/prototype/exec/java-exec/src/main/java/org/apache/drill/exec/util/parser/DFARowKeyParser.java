package org.apache.drill.exec.util.parser;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/5/13
 * Time: 2:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class DFARowKeyParser {
    private static Logger logger = LoggerFactory.getLogger(DFARowKeyParser.class);

    private DFA dfa;
    private DFA.DFAMatcher matcher = null;
    private List<KeyPart> primaryRowKeyParts;
    private Map<String, HBaseFieldInfo> rkFieldInfoMap;

    //可确定的固定长度字段位置，不用DFA解析
    private Map<String, Pair<Integer, Integer>> constField = new HashMap<>();

    public long parseDFACost = 0;
    public long parseAndSetValCost = 0;

    public DFARowKeyParser(List<KeyPart> primaryRowKeyParts, Map<String, HBaseFieldInfo> rkFieldInfoMap){
      this.primaryRowKeyParts = primaryRowKeyParts;
      this.rkFieldInfoMap = rkFieldInfoMap;
      this.dfa = new DFA(this.primaryRowKeyParts, this.rkFieldInfoMap);
      this.matcher = dfa.newMatcher(null);
      initConstField();
    }

  public Map<String, Pair<Integer, Integer>> getConstField() {
    return constField;
  }

  public void initConstField() {
      int i = 0;
      int startPos = 0;
      //正向扫描确定固定长度投影
      for (; i<primaryRowKeyParts.size(); i++) {
        KeyPart kp = primaryRowKeyParts.get(i);
        if (kp.getField() == null) {
          break;
        }
        String colName = kp.getField().getName();
        HBaseFieldInfo info = rkFieldInfoMap.get(colName);
        int len = info.serLength;
        //遇到变长子段结束
        if (len <= 0) {
          break;
        }
        //在row key中表示一个投影字段，记录下来
        if (kp.getType() == KeyPart.Type.field) {
          Pair<Integer, Integer> posPair = new Pair<>(startPos, startPos + len);
          constField.put(colName, posPair);
        }
        startPos = startPos + len;
      }
      //全部扫描结束
      if (i == primaryRowKeyParts.size()) {
        return;
      }
      //反向扫描确定固定长度投影
      startPos = 0;
      for (i=primaryRowKeyParts.size()-1; i>=0; i--) {
        KeyPart kp = primaryRowKeyParts.get(i);
        if (kp.getField() == null) {
          break;
        }
        String colName = kp.getField().getName();
        HBaseFieldInfo info = rkFieldInfoMap.get(colName);
        int len = info.serLength;
        if (len <= 0) {
          break;
        }
        if (kp.getType() == KeyPart.Type.field) {
          Pair<Integer, Integer> posPair = new Pair<>(startPos-len, startPos);
          constField.put(colName, posPair);
        }
        startPos = startPos - len;
      }

    }

    public void parseAndSet(byte[] rk, Map<String, HBaseFieldInfo> projs, Map<String, ValueVector> vvMap, int vvIndex, boolean useDFA) {
      //不用dfa解析，row key中投影字段可直接完全解析
      if (!useDFA) {
        for (Map.Entry<String, HBaseFieldInfo> entry : projs.entrySet()) {
          String colName = entry.getKey();
          HBaseFieldInfo info = entry.getValue();
          Pair<Integer, Integer> posInfo = constField.get(colName);
          int startPos = posInfo.getFirst()<=0 ? posInfo.getFirst() + rk.length : posInfo.getFirst();
          int endPos = posInfo.getSecond()<=0 ? posInfo.getSecond() + rk.length : posInfo.getSecond();
          Object o = null;
          if(info.serType == HBaseFieldInfo.DataSerType.BINARY) {
            o = parseBytes(rk, startPos, endPos, info.getDataType());
          } else {
            if (info.getDataType() == HBaseFieldInfo.DataType.STRING) {
              //string类型直接返回byte[]，提供给value vector存储
              o = parseBytes(rk, startPos, endPos, info.getDataType());
            } else {
              o = parseString
                      (decodeText(rk, startPos, endPos), info.getDataType());
            }
          }
          ValueVector vv = vvMap.get(colName);
          vv.getMutator().setObject(vvIndex, o);
        }
      } else {
        matcher.resetTo(rk);
        //记录每个col name所对应的在原始row key中的位置和key part信息
        Map<String, DFA.FieldPosition> keyPartInfos = new HashMap<>();
        DFA.FieldPosition nextField = matcher.nextField();
        while (nextField != null){
          keyPartInfos.put(nextField.fieldDef.getField().getName(), nextField);
          nextField = matcher.nextField();
        }

        //如果需要此字段的投影才解析
        for (Map.Entry<String, HBaseFieldInfo> entry : projs.entrySet()) {
          String colName = entry.getKey();
          HBaseFieldInfo info = entry.getValue();

          DFA.FieldPosition posInfo = keyPartInfos.get(colName);
          if (posInfo == null) {
            throw new NullPointerException(colName + "'s postion info is null! Row key: " + Bytes.toStringBinary(rk));
          }
          Object o = null;
          if(info.serType == HBaseFieldInfo.DataSerType.BINARY) {
            o = parseBytes(rk, posInfo.start, posInfo.end, info.getDataType());
          } else {
            if (info.getDataType() == HBaseFieldInfo.DataType.STRING) {
              //string类型直接返回byte[]，提供给value vector存储
              o = parseBytes(rk, posInfo.start, posInfo.end, info.getDataType());
            } else {
              o = parseString
                    (decodeText(rk, posInfo.start, posInfo.end), info.getDataType());
            }
          }
          ValueVector vv = vvMap.get(colName);
          vv.getMutator().setObject(vvIndex, o);
        }
      }
    }

    static String decodeText(byte[] bytes, int start, int end){
        char[] chars = new char[end-start];
        int index = start;
        for(int i=0; i<chars.length; i++){
            chars[i] = (char) bytes[index++];
        }
        return new String(chars);
    }

    public static Object parseBytes(byte[] orig, HBaseFieldInfo.DataType type){
        switch (type) {
            case INT:
              return Bytes.toInt(orig);
            case SMALLINT:
              return Bytes.toShort(orig);
            case TINYINT:
              return orig[0];
            case STRING:
              return orig;
            case BIGINT:
              return Bytes.toLong(orig);
        }
        return null;
    }
  public static int toInt(byte[] bytes, int start){
    int a = (0xff & bytes[start])<<24;
    int b = (0xff & bytes[start+1])<<16;
    int c = (0xff & bytes[start+2])<<8;
    int d = (0xff & bytes[start+3]);
//    a = a|b;
//    c = c|d;
    return a|b|c|d;
//    return bytes[start]<<24 + bytes[start+1]<<16 + bytes[start+2]<<8 + bytes[start+3];
  }
  
    public static Object parseBytes(byte[] orig, int start, int end, HBaseFieldInfo.DataType type) {
        switch (type) {
            case INT:
              return Bytes.toInt(orig, start);
            case SMALLINT:
              return Bytes.toShort(orig, start);
            case TINYINT:
              return orig[0];
            case STRING:
              byte[] result;
              int len = end-start;
              result = new byte[len];
              System.arraycopy(orig, start, result, 0, len);
              return result;
            case BIGINT:
              return Bytes.toLong(orig, start);
        }
        return null;
    }

    public static Object parseString(String orig, HBaseFieldInfo.DataType type){
        switch (type) {
            case INT:
                return Integer.parseInt(orig);
            case TINYINT:
                return orig.charAt(0);
            case SMALLINT:
                return (short)Integer.parseInt(orig);
            case STRING:
                return orig;
            case BIGINT:
                return Long.parseLong(orig);
        }
        return null;
    }

}
