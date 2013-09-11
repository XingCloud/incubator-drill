package com.xingcloud.hbase.util;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import org.apache.hadoop.hbase.util.Bytes;
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
    private Map<String,HBaseFieldInfo> rkFieldInfoMap;

    public long constructStateCost = 0;
    public long parseValueCost = 0;
    public long parseBytesCost = 0;
    public long parseStrCost = 0;



    public DFARowKeyParser(List<KeyPart> primaryRowKeyParts,Map<String,HBaseFieldInfo> rkFieldInfoMap){
        this.primaryRowKeyParts=primaryRowKeyParts;
        this.rkFieldInfoMap=rkFieldInfoMap;
        this.dfa=new DFA(this.primaryRowKeyParts,this.rkFieldInfoMap);
    }

    public  Map<String,Object> parse(byte[] rk){
        Map<String,Object> rkObjectMap = new HashMap<>();
        DFA.State prev = dfa.begin().directNext;
        DFA.State next;
        DFA.State end = dfa.end();
        int[] offsets = new int[20];
        KeyPart[] Kps = new KeyPart[20];
        int len = 1;
        int index = 0;
        offsets[0] = 0;
        long st = System.nanoTime();
        while(index < rk.length){
            next = dfa.next(prev,rk[index]);
            if (next != prev){
                offsets[len] = index;
                Kps[len++] = prev.kp;
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
            prev.len = 0;
        }
        constructStateCost += System.nanoTime() - st;

        st = System.nanoTime();
        for(int i=1; i<len+1; i++){
            KeyPart kp = Kps[i];
            if(kp.getType() == KeyPart.Type.field){
                java.lang.String colName = kp.getField().getName();
                HBaseFieldInfo info = rkFieldInfoMap.get(colName);

                if(info.serType == HBaseFieldInfo.DataSerType.BINARY) {
                    long stTmp = System.nanoTime();

                    java.lang.Object o = parseBytes(rk, offsets[i-1], offsets[i], kp.getField().getType());
                    parseBytesCost += System.nanoTime() - stTmp;
                    rkObjectMap.put(colName,o);
                } else {
                    long stTmp = System.nanoTime();
                    java.lang.Object o = parseString
                            (decodeText(rk, offsets[i-1], offsets[i]), kp.getField().getType());
                    parseStrCost += System.nanoTime() - stTmp;
                    rkObjectMap.put(colName,o);
                }
            }
        }
        parseValueCost += System.nanoTime() - st;
        dfa.reset();
        return rkObjectMap;
    }

    static String decodeText(byte[] bytes, int start, int end){
        char[] chars = new char[end-start];
        int index = start;
        for(int i=0; i<chars.length; i++){
            chars[i] = (char) bytes[index++];
        }
        return new String(chars);
    }

    public static Object parseBytes(byte[] orig, String type){
        byte[] result;
        switch (type) {
            case "int":
                result = new byte[4];
                for (int i = 0; i < 4 - orig.length; i++)
                    result[i] = 0;
                for (int i = 4 - orig.length; i < 4; i++)
                    result[i] = orig[i];
                return Bytes.toInt(result);
            case "smallint":
                result=new byte[2];
                for(int i=0;i<2-orig.length;i++){
                    result[i]=0;
                }
                for(int i=2-orig.length;i<2;i++){
                    result[i]=orig[i];
                }
                return Bytes.toShort(result);
            case "tinyint":
                return orig[0];
            case "string":
                return Bytes.toString(orig);
            case "bigint":
                result = new byte[8];
                for (int i = 0; i < 8 - orig.length; i++)
                    result[i] = 0;
                for (int i = 8 - orig.length; i < 8; i++)
                    result[i] = orig[i];
                return Bytes.toLong(result);
        }
        return null;
    }

    public static Object parseBytes(byte[] orig, int start, int end, String type) {
        byte[] result;
        int index = start;
        int len = end-start;
        switch (type) {
            case "int":
                result = new byte[4];
                System.arraycopy(orig, start, result, 4-len, len);
//                for (int i = 0; i < 4 - len; i++)
//                    result[i] = 0;
//                for (int i = 4 - len; i < 4; i++)
//                    result[i] = orig[index++];
                return Bytes.toInt(result);
            case "smallint":
                result = new byte[2];
                System.arraycopy(orig, start, result, 2-len, len);
//                for(int i=0;i<2-len;i++){
//                    result[i]=0;
//                }
//                for(int i=2-len;i<2;i++){
//                    result[i]=orig[index++];
//                }
                return Bytes.toShort(result);
            case "tinyint":
                return orig[0];
            case "string":
                return Bytes.toString(orig);
            case "bigint":
                result = new byte[8];
                System.arraycopy(orig, start, result, 8-len, len);
//                for (int i = 0; i < 8 - len; i++)
//                    result[i] = 0;
//                for (int i = 8 - len; i < 8; i++)
//                    result[i] = orig[index++];
                return Bytes.toLong(result);
        }
        return null;
    }

    public static Object parseString(String orig, String type){
        switch (type) {
            case "int":
                return Integer.parseInt(orig);
            case "tinyint":
                return type.charAt(0);
            case "smallint":
                return (short)Integer.parseInt(orig);
            case "string":
                return orig;
            case "bigint":
                return Long.parseLong(orig);
        }
        return null;
    }

}
