package com.xingcloud.hbase.util;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 9/4/13
 * Time: 9:25 AM
 */
public class RowKeyUtils {

  public static byte[] appendBytes(byte[] orig, byte[] tail) {
    byte[] result = new byte[orig.length + tail.length];
    System.arraycopy(orig,0,result,0,orig.length);
    System.arraycopy(tail,0,result,orig.length,tail.length);
    return result ;
  }

  public static byte[] produceTail(boolean start) {
    if(start){
      return new byte[]{0,0,0,0,0};
    }else{
      return new byte[]{-1,-1,-1,-1,-1};
    }
  }

  public static String processRkBound(String rk,boolean start){
     if(start){
         if(rk.endsWith(".\\xFF\\x00\\x00\\x00\\x00\\x00"))
             return rk;
         else if(rk.endsWith(".\\xFF"))
             return rk.concat("\\x00\\x00\\x00\\x00\\x00");
         else
             return rk.concat(".\\xFF\\x00\\x00\\x00\\x00\\x00");
     }else {
         if(rk.endsWith(".\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF"))
             return rk;
         else if(rk.endsWith(".\\xFF"))
             return rk.concat("\\xFF\\xFF\\xFF\\xFF\\xFF");
         else
             return rk.concat("\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF");
     }
  }

}
