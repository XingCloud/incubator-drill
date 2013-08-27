package com.xingcloud.hbase.util;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/31/13
 * Time: 5:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowKeyParser {

    public static Map<String,Object> parse(byte[] rk,List<KeyPart> primaryRowkeyParts,
                                                 Map<String,HBaseFieldInfo> rkFieldInfoMap){
         Map<String,Object> rkObjectMap=new HashMap<>();
         int index=0;
         parseRkey(rk,false,primaryRowkeyParts,null,rkFieldInfoMap,rkObjectMap,index);
         return rkObjectMap;
    }

    public static int parseRkey(byte[] rk, boolean optional, List<KeyPart> keyParts, KeyPart endKeyPart,
                                Map<String,HBaseFieldInfo> rkFieldInfoMap,Map<String, Object> rkObjectMap,int index) {
        int fieldEndindex = index;
        byte[] result;
        String ret;
        Object o;
        for (int i = 0; i < keyParts.size(); i++) {
            KeyPart kp = keyParts.get(i);
            switch (kp.getType()){
                case field:
                    HBaseFieldInfo info = rkFieldInfoMap.get(kp.getField().getName());
                    switch (info.serType){
                        case TEXT:
                            if(info.serLength==0)continue;
                            fieldEndindex = index + info.serLength;
                            if (optional && fieldEndindex > rk.length) return index;
                            result = Arrays.copyOfRange(rk, index, fieldEndindex);
                            ret = Bytes.toString(result);
                            o=parseString(ret,info.fieldSchema.getType());
                            rkObjectMap.put(info.fieldSchema.getName(),o);
                            index = fieldEndindex;
                            break;
                        case WORD:
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
                                    if (endKeyPart.getConstant().equals("\\xFF")) endCons[0] = -1;
                                    while (fieldEndindex < rk.length && rk[fieldEndindex] != endCons[0]) {
                                        fieldEndindex++;
                                    }
                                }
                            }
                            if (fieldEndindex != index) {
                                result = Arrays.copyOfRange(rk, index, fieldEndindex);
                                ret = Bytes.toString(result);
                                o=parseString(ret,info.fieldSchema.getType());
                                rkObjectMap.put(info.fieldSchema.getName(),o);
                                index = fieldEndindex;
                            } else {
                                return index;
                            }
                            break;
                        case BINARY:
                            if(info.serLength==0)continue;
                            fieldEndindex = index + info.serLength;
                            if (fieldEndindex > rk.length) return index;
                            //result = Arrays.copyOfRange(rk, index, fieldEndindex);
                            Object ob = parseBytes(rk,index,fieldEndindex, info.fieldSchema.getType());
                            rkObjectMap.put(info.fieldSchema.getName(), ob);
                            index=fieldEndindex;

                    }
                    break;
                case optionalgroup:
                    List<KeyPart> optionalKeyParts = kp.getOptionalGroup();
                    KeyPart endKp;
                    if (optional == false) endKp = keyParts.get(i + 1);
                    else endKp = endKeyPart;
                    index=parseRkey(rk, true, optionalKeyParts, endKp, rkFieldInfoMap,rkObjectMap,index);
                    break;
                case constant:
                     index+=1;
                     break;
            }
        }
        return index;
    }

    public static Object parseBytes(byte[] orig, int start,int end,String type) {
        byte[] result;
        int index = start;
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
