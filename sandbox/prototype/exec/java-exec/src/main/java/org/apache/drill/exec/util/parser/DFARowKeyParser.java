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

    private static final int INT_BYTE_SIZE = 4;
    private static final int LONG_BYTE_SIZE = 8;
    private static final int SMALLINT_BYTE_SIZE = 2;


    private DFA dfa;
    private List<KeyPart> primaryRowKeyParts;
    private Map<String,HBaseFieldInfo> rkFieldInfoMap;

    public DFARowKeyParser(List<KeyPart> primaryRowKeyParts, Map<String, HBaseFieldInfo> rkFieldInfoMap){
        this.primaryRowKeyParts = primaryRowKeyParts;
        this.rkFieldInfoMap = rkFieldInfoMap;
        this.dfa = new DFA(this.primaryRowKeyParts, this.rkFieldInfoMap);
    }

    public void parseAndSet(byte[] rk, Map<String, HBaseFieldInfo> projs, Map<String, ValueVector> vvMap, int vvIndex) {
        DFA.State prev = dfa.begin().directNext;
        DFA.State next;
        DFA.State end = dfa.end();
        int[] offsets = new int[20];
        KeyPart[] Kps = new KeyPart[20];
        //记录每个col name所对应的在原始row key中的位置和key part信息
        Map<String, Pair<Pair<Integer, Integer>, KeyPart>> keyPartInfos = new HashMap<>();
        int len = 1;
        int index = 0;
        offsets[0] = 0;
        while(index < rk.length) {
            next = dfa.next(prev,rk[index]);
            if (next != prev){
                offsets[len] = index;
                Kps[len++] = prev.kp;
                if (len > 1 && prev.kp.getType() == KeyPart.Type.field) {
                    String colName = prev.kp.getField().getName();
                    Pair<Integer, Integer> posPair = new Pair<>(offsets[len-2], offsets[len-1]);
                    Pair<Pair<Integer, Integer>, KeyPart> keyPartInfo = new Pair<>(posPair, prev.kp);
                    keyPartInfos.put(colName, keyPartInfo);
                }
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
            offsets[len] = rk.length;
            Kps[len] = prev.kp;

            if (len > 1 && prev.kp.getType() == KeyPart.Type.field) {
              String colName = prev.kp.getField().getName();
              Pair<Integer, Integer> posPair = new Pair<>(offsets[len-1], offsets[len]);
              Pair<Pair<Integer, Integer>, KeyPart> keyPartInfo = new Pair<>(posPair, prev.kp);
              keyPartInfos.put(colName, keyPartInfo);
            }

            //下次解析起始len为0
            prev.len = 0;
        }

        //如果需要此字段的投影才解析
        for (Map.Entry<String, HBaseFieldInfo> entry : projs.entrySet()) {
          String colName = entry.getKey();
          HBaseFieldInfo info = entry.getValue();

          Pair<Pair<Integer, Integer>, KeyPart> keyPart = keyPartInfos.get(colName);
          Pair<Integer, Integer> posInfo = keyPart.getFirst();
          KeyPart kp = keyPart.getSecond();

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
        byte[] result;
        int len = orig.length;
        switch (type) {
            case INT:
              result = new byte[INT_BYTE_SIZE];
              System.arraycopy(orig, 0, result, INT_BYTE_SIZE-len, len);
              return Bytes.toInt(result);
            case SMALLINT:
              result = new byte[SMALLINT_BYTE_SIZE];
              System.arraycopy(orig, 0, result, SMALLINT_BYTE_SIZE-len, len);
              return Bytes.toShort(result);
            case TINYINT:
                return orig[0];
            case STRING:
                return orig;
            case BIGINT:
              result = new byte[LONG_BYTE_SIZE];
              System.arraycopy(orig, 0, result, LONG_BYTE_SIZE-len, len);
              return Bytes.toLong(result);
        }
        return null;
    }

    public static Object parseBytes(byte[] orig, int start, int end, HBaseFieldInfo.DataType type) {
        byte[] result;
        int len = end-start;
        switch (type) {
            case INT:
                result = new byte[INT_BYTE_SIZE];
                System.arraycopy(orig, start, result, INT_BYTE_SIZE-len, len);
                return Bytes.toInt(result);
            case SMALLINT:
                result = new byte[SMALLINT_BYTE_SIZE];
                System.arraycopy(orig, start, result, SMALLINT_BYTE_SIZE-len, len);
                return Bytes.toShort(result);
            case TINYINT:
                return orig[0];
            case STRING:
                result = new byte[len];
                System.arraycopy(orig, start, result, 0, len);
                return result;
            case BIGINT:
                result = new byte[LONG_BYTE_SIZE];
                System.arraycopy(orig, start, result, LONG_BYTE_SIZE-len, len);
                return Bytes.toLong(result);
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
