package org.apache.drill.exec.physical.impl.vector;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarBigIntVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.junit.Assert.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-2
 * Time: 下午6:23
 * To change this template use File | Settings | File Templates.
 */
public class TestVarBigIntVector extends PopUnitTestBase {
  public Logger logger= org.slf4j.LoggerFactory.getLogger(TestVarBigIntVector.class);

  @Test
  public void addDataTest() {
    long[] numbers = {0l, -1l, 1l, -2l, 2l, -3l, 3l, -10l, 10l, 2147483647l, -2147483648l, 4294967294l, -Long.MAX_VALUE/2, -Long.MAX_VALUE/2, -320l, 320};
    DrillConfig config = DrillConfig.create();
    BufferAllocator allocator = BufferAllocator.getAllocator(config);
    VarBigIntVector vv = new VarBigIntVector(null, allocator);
    vv.allocateNew(numbers.length*8/2, numbers.length);
    VarBigIntVector.Mutator mutator = vv.getMutator();
    mutator.setValueCount(numbers.length);
    for (int i=0; i<numbers.length; i++) {
      mutator.set(i, numbers[i]);
    }

    VarBigIntVector.Accessor accessor = vv.getAccessor();
    for (int i=0; i<accessor.getValueCount(); i++) {
      long val = accessor.get(i);
      assertEquals(numbers[i], val);
    }

  }

}
