package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.HBaseEventUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/31/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestParseRowkey {

    private int index=0;
    private Map<String, HBaseFieldInfo> fieldInfoMap;
    private Map<String, Object> rkObjectMap;
    private List<KeyPart> primaryRowKeyParts;

    @Test
    public void parseRowKey(){
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        String day="20120312";
        String event="audit.consume.coin.";
        byte[] uid=Bytes.toBytes((int)14000);
        byte uhash=(byte)20;
        byte[] iuid=new byte[5];
        iuid[0]=uhash;
        for(int i=0;i<4;i++)iuid[i+1]=uid[i];
        byte[] rk= HBaseEventUtils.getRowKey(Bytes.toBytes(day),event,iuid);
        for(int i=0;i<10000;i++){
              parseRkey(rk,false,primaryRowKeyParts,null,rkObjectMap);
        }
    }

    private void init() throws Exception {
        String tableName="testtable100W_deu";
        String[]projections={"event0","uid","value"};
        List<String> options=Arrays.asList(projections);
        List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, options);
        for (HBaseFieldInfo col : cols) {
            fieldInfoMap.put(col.fieldSchema.getName(), col);
        }
        primaryRowKeyParts=TableInfo.getRowKey(tableName,options);
        rkObjectMap=new HashMap<>();
    }

    private void parseRkey(byte[] rk, boolean optional, List<KeyPart> keyParts, KeyPart endKeyPart,
                           Map<String, Object> rkObjectMap) {
        int fieldEndindex = index;
        for (int i = 0; i < keyParts.size(); i++) {
            KeyPart kp = keyParts.get(i);
            if (kp.getType() == KeyPart.Type.field) {
                HBaseFieldInfo info = fieldInfoMap.get(kp.getField().getName());
                if (info.serType == HBaseFieldInfo.DataSerType.TEXT
                        && info.serLength != 0) {
                    fieldEndindex = index + info.serLength;
                    if (optional && fieldEndindex > rk.length) return;
                    byte[] result = Arrays.copyOfRange(rk, index, fieldEndindex);
                    String ret = Bytes.toString(result);
                    Object o=parseString(ret,info.fieldSchema.getType());
                    rkObjectMap.put(info.fieldSchema.getName(),o);
                    index = fieldEndindex;
                } else if (info.serType == HBaseFieldInfo.DataSerType.WORD) {
                    if (i < keyParts.size() - 1) {
                        KeyPart nextkp = keyParts.get(i + 1);
                        String nextCons = nextkp.getConstant();
                        byte[] nextConsBytes = Bytes.toBytes(nextCons);

                        if (optional) {
                            byte[] endCons = Bytes.toBytes(endKeyPart.getConstant());
                            if (endKeyPart.getConstant().equals("\\xFF")) endCons[0] = -1;
                            while (fieldEndindex < rk.length && rk[fieldEndindex] != nextConsBytes[0] &&
                                    rk[fieldEndindex] != endCons[0]) {
                                fieldEndindex++;
                            }
                        } else
                            while (fieldEndindex < rk.length && rk[fieldEndindex] != nextConsBytes[0]) {
                                fieldEndindex++;
                            }
                    } else {
                        if (endKeyPart == null)
                            fieldEndindex = rk.length;
                        else {
                            byte[] endCons = Bytes.toBytes(endKeyPart.getConstant());
                            while (fieldEndindex < rk.length && rk[fieldEndindex] != endCons[0]) {
                                fieldEndindex++;
                            }
                        }
                    }
                    if (fieldEndindex != index) {
                        byte[] result = Arrays.copyOfRange(rk, index, fieldEndindex);
                        String ret = Bytes.toString(result);
                        Object o=parseString(ret,info.fieldSchema.getType());
                        rkObjectMap.put(info.fieldSchema.getName(),o);
                        index = fieldEndindex;
                    } else {
                        return;
                    }

                } else if (info.serType == HBaseFieldInfo.DataSerType.BINARY && info.serLength != 0) {
                    fieldEndindex = index + info.serLength;
                    if (optional && fieldEndindex > rk.length) return;
                    byte[] result;
                    result = Arrays.copyOfRange(rk, index, fieldEndindex);
                    Object ob = parseBytes(result, info.fieldSchema.getType());
                    rkObjectMap.put(info.fieldSchema.getName(), ob);
                    index=fieldEndindex;
                }
            } else if (kp.getType() == KeyPart.Type.optionalgroup) {
                List<KeyPart> optionalKeyParts = kp.getOptionalGroup();
                KeyPart endKp;
                if (optional == false) endKp = keyParts.get(i + 1);
                else endKp = endKeyPart;
                parseRkey(rk, true, optionalKeyParts, endKp, rkObjectMap);

            } else if (kp.getType() == KeyPart.Type.constant) {
                index++;
            }
        }
    }

    private Object parseString(String orig, String type){
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

    private Object parseBytes(byte[] orig, String type) {
        byte[] result;
        int index = 0;
        switch (type) {
            case "int":
                result = new byte[4];
                for (int i = 0; i < 4 - orig.length; i++)
                    result[i] = 0;
                for (int i = 4 - orig.length; i < 4; i++)
                    result[i] = orig[index++];
                return Bytes.toInt(result);
            case "smallint":
                result=new byte[2];
                for(int i=0;i<2-orig.length;i++){
                    result[i]=0;
                }
                for(int i=2-orig.length;i<2;i++){
                    result[i]=orig[index++];
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
                    result[i] = orig[index++];
                return Bytes.toLong(result);
        }
        return null;
    }
}
