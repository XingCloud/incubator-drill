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
      return new byte[]{'.',-1,0,0,0,0,0};
    }else{
      return new byte[]{'.',-1,-1,-1,-1,-1,-1};
    }
  }

}
