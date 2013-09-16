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
    private List<KeyPart> primaryRowKeyParts;
    private Map<String, HBaseFieldInfo> rkFieldInfoMap;

    public long parseDFACost = 0;
    public long parseAndSetValCost = 0;

    public DFARowKeyParser(List<KeyPart> primaryRowKeyParts, Map<String, HBaseFieldInfo> rkFieldInfoMap){
        this.primaryRowKeyParts = primaryRowKeyParts;
        this.rkFieldInfoMap = rkFieldInfoMap;
        this.dfa = new DFA(this.primaryRowKeyParts, this.rkFieldInfoMap);
    }

    public void parseAndSet(byte[] rk, Map<String, HBaseFieldInfo> projs, Map<String, ValueVector> vvMap, int vvIndex) {
        long st = System.nanoTime();
        DFA.State prev = dfa.begin().directNext;
        DFA.State next;
        DFA.State end = dfa.end();
        //记录每个col name所对应的在原始row key中的位置和key part信息
        Map<String, Pair<Integer, Integer>> keyPartInfos = new HashMap<>();
        int len = 1;
        int index = 0;
        int lastIndex = 0;
        while(index < rk.length) {
            next = dfa.next(prev,rk[index]);
            if (next != prev){
                len++;
                if (len > 1 && prev.kp.getType() == KeyPart.Type.field) {
                    String colName = prev.kp.getField().getName();
                    Pair<Integer, Integer> posPair = new Pair<>(lastIndex, index);
                    keyPartInfos.put(colName, posPair);
                }
                lastIndex = index;
                //下次解析起始len为0
                prev.len = 0;
                index--;
                prev = next;
                if(prev == end){
                    len--;
                    break;
                }
            }
            index++;
            if(prev.size > prev.len)
            {
                index += (prev.size-prev.len);
                prev.len = prev.size;
            }
        }
        if(prev != end){
            if (len > 1 && prev.kp.getType() == KeyPart.Type.field) {
              String colName = prev.kp.getField().getName();
              Pair<Integer, Integer> posPair = new Pair<>(lastIndex, rk.length);
              keyPartInfos.put(colName, posPair);
            }

            //下次解析起始len为0
            prev.len = 0;
        }
        parseDFACost += System.nanoTime() - st;

        st = System.nanoTime();
        //如果需要此字段的投影才解析
        for (Map.Entry<String, HBaseFieldInfo> entry : projs.entrySet()) {
          String colName = entry.getKey();
          HBaseFieldInfo info = entry.getValue();

          Pair<Integer, Integer> posInfo = keyPartInfos.get(colName);

          Object o = null;
          if(info.serType == HBaseFieldInfo.DataSerType.BINARY) {
            o = parseBytes(rk, posInfo.getFirst(), posInfo.getSecond(), info.getDataType());
          } else {
            if (info.getDataType() == HBaseFieldInfo.DataType.STRING) {
              //string类型直接返回byte[]，提供给value vector存储
              o = parseBytes(rk, posInfo.getFirst(), posInfo.getSecond(), info.getDataType());
            } else {
              o = parseString
                    (decodeText(rk, posInfo.getFirst(), posInfo.getSecond()), info.getDataType());
            }
          }
          ValueVector vv = vvMap.get(colName);
          vv.getMutator().setObject(vvIndex, o);
        }
        parseAndSetValCost += System.nanoTime() - st;
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
