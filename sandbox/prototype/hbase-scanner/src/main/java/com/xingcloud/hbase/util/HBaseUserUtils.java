package com.xingcloud.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/26/13
 * Time: 8:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUserUtils {
    private static Log logger = LogFactory.getLog(HBaseUserUtils.class);
    public static byte[] getRowKey(byte[] propId,byte[] date,byte[] val){
         int length=propId.length+date.length+val.length;
         byte[] rk=new byte[length];
         int index=0;
         for(int i=0;i<propId.length;i++){
             rk[index++]=propId[i];
         }
         for(int i=0;i<date.length;i++){
             rk[index++]=date[i];
         }
         for(int i=0;i<val.length;i++){
             rk[index++]=val[i];
         }
        return rk;
    }

    public static byte[] getRowKey(byte[] propId,byte[] date){
        int length=propId.length+date.length;
        byte[] rk=new byte[length];
        int index=0;
        for(int i=0;i<propId.length;i++){
            rk[index++]=propId[i];
        }
        for(int i=0;i<date.length;i++){
            rk[index++]=date[i];
        }
        return rk;
    }

}
