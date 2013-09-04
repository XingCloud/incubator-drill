package com.xingcloud.xa.meta;

import com.xingcloud.meta.ByteUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestByteUtils {
  
  @Test
  public void testCompareBytes()throws Exception{
    String s1 = "20130904~iwit.heartbeat";
    String s2 = "20130904\\xC3\\xA6\\xC2\\xA5\\xC2\\xB6\\xC3\\xA6\\xC2\\xA5\\xC2\\xB3\\xC3\\xA2\\xC2\\xB9\\xC2\\xB4\\xC3\\xA6\\xE2\\x80\\xA2\\xC2\\xA8\\xC3\\xA7\\xE2\\x80\\xB0\\xC2\\xA1\\xC3\\xA6\\xE2\\x80\\xB0\\xC2\\xB4\\xC3\\xA6\\xE2\\x80\\xA6";
    byte[] b1 =     ByteUtils.toBytesBinary(s1);
    byte[] b2 = ByteUtils.toBytesBinary(s2);
    Assert.assertTrue(ByteUtils.compareBytes(b1, b2)<0); 
    
  }
}
