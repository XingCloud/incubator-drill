package org.apache.drill.exec.util;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.memory.SingleAllocator;
import org.apache.drill.exec.util.hash.OffHeapIntIntOpenHashMap;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: liuxiong
 * Date: 13-9-9
 * Time: 下午5:53
 */
public class TestOffHeapIntIntOpenHashMap {

  private final BufferAllocator bufferAllocator = new DirectBufferAllocator();

  @Test
  public void testPutAndGet(){
    final int NUM_KEYS = 1000000;
    SingleAllocator singleAllocator = new SingleAllocator(bufferAllocator);
    OffHeapIntIntOpenHashMap map = new OffHeapIntIntOpenHashMap(singleAllocator);

    assertTrue(map.isEmpty());

    long start = System.currentTimeMillis();
    for (int i = -NUM_KEYS; i < NUM_KEYS; i++) {
      map.put(i, i + 1);
    }
    System.out.println("time cost: " + (System.currentTimeMillis() - start));

    assertEquals(map.size(), NUM_KEYS * 2);

    for (int i = -NUM_KEYS; i < NUM_KEYS; i++) {
      assertEquals(map.get(i), i + 1);
    }

    for (int i = - 2 * NUM_KEYS; i < 2 * NUM_KEYS; i++){
      if(map.containsKey(i)){
        assertEquals(map.lget(), i + 1);
      }
    }

    map.release();
    map.release();
    map.release();
    singleAllocator.close();
  }

  @Test
  public void testClear(){
    SingleAllocator singleAllocator = new SingleAllocator(bufferAllocator);
    OffHeapIntIntOpenHashMap map = new OffHeapIntIntOpenHashMap(singleAllocator);

    final int size = 10;
    for (int i = 0; i < size; i++) {
      map.put(i, i * i);
    }
    for (int i = 0; i < size; i++) {
      assertEquals(map.get(i), i * i);
    }

    assertEquals(map.size(), size);

    map.clear();

    assertTrue(map.isEmpty());

    for (int i = 0; i < size * 2; i++) {
      map.put(i, i + 1);
    }
    for (int i = 0; i < size * 2; i++) {
      assertEquals(map.get(i), i + 1);
    }

    assertEquals(map.size(), size * 2);

    map.release();
    singleAllocator.close();
  }

  public static void main(String[] args){
    TestOffHeapIntIntOpenHashMap testMap = new TestOffHeapIntIntOpenHashMap();
    for (int i = 0; i < 10000; i++) {
      testMap.testClear();
      testMap.testPutAndGet();
    }
  }

}
