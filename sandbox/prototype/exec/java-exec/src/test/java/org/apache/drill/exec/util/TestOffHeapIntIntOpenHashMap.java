package org.apache.drill.exec.util;

//import com.carrotsearch.hppc.IntIntOpenHashMap;
import org.apache.drill.exec.util.hash.IntIntOpenHashMap;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
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
  private final int MAX_KEY = 1000000;
  private final int MIN_KEY = -MAX_KEY;
  private final int NUM_KEYS = MAX_KEY - MIN_KEY;


  @Test
  public void testPutAndGetOffHeap(){
    OffHeapIntIntOpenHashMap map = new OffHeapIntIntOpenHashMap(bufferAllocator);

    assertTrue(map.isEmpty());

    long start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++) {
      map.put(i, i + 1);
    }
    System.out.println("<OFF-HEAP> put operation, time cost: " + (System.currentTimeMillis() - start));

    assertEquals(map.size(), NUM_KEYS);

//    start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++) {
      assertEquals(map.get(i), i + 1);
    }
//    System.out.println("<OFF-HEAP> get operation, time cost: " + (System.currentTimeMillis() - start));

//    start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++){
      if(map.containsKey(i)){
        assertEquals(map.lget(), i + 1);
      }
    }
//    System.out.println("<OFF-HEAP> lget operation, time cost: " + (System.currentTimeMillis() - start));

    map.release();
  }

  @Test
  public void testClearOffHeap(){
    OffHeapIntIntOpenHashMap map = new OffHeapIntIntOpenHashMap(bufferAllocator);

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
  }

  public void testPutAndGetOnHeap(){
    IntIntOpenHashMap map = new IntIntOpenHashMap();

    assertTrue(map.isEmpty());

    long start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++) {
      map.put(i, i + 1);
    }
    System.out.println("<ON-HEAP> put operation, time cost: " + (System.currentTimeMillis() - start));

    assertEquals(map.size(), NUM_KEYS);

//    start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++) {
      assertEquals(map.get(i), i + 1);
    }
//    System.out.println("<ON-HEAP> get operation, time cost: " + (System.currentTimeMillis() - start));

//    start = System.currentTimeMillis();
    for (int i = MIN_KEY; i < MAX_KEY; i++){
      if(map.containsKey(i)){
        assertEquals(map.lget(), i + 1);
      }
    }
//    System.out.println("<ON-HEAP> lget operation, time cost: " + (System.currentTimeMillis() - start));
  }

  public static void main(String[] args){
    TestOffHeapIntIntOpenHashMap testMap = new TestOffHeapIntIntOpenHashMap();
    for (int i = 0; i < 10000; i++) {
      testMap.testPutAndGetOffHeap();
      testMap.testPutAndGetOnHeap();
      System.out.println();
    }
  }

}
