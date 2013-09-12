package org.apache.drill.exec.test;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Test;

public class TestVector {
  @Test
  public void testInt()throws Exception{
    int size = 1000000;
    int round = 100;
    BufferAllocator allocator = BufferAllocator.getAllocator(null);
    IntVector vector = new IntVector(null, allocator);
    vector.allocateNew(size);
    int[] array = new int[size];
    int asum = 0, vsum = 0;
    vsum = testVector(vector, size, round);
    asum = testArray(array, size, round);
    System.out.println(""+vsum+","+asum);
    long t0 = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      vsum = testVector(vector, size, round);
    }
    long t1 = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      asum = testArray(array, size, round);
    }
    long t2 = System.nanoTime();
    System.out.println(""+vsum+"vtime:\t"+(t1-t0)/1000000+"\tms,"+asum+"atime:\t"+(t2-t1)/1000000+"\tms.");
  }

  private int testArray(int[] array, int size, int round) {
    int sum = 0;
    for (int j = 0; j < round; j++) {
      sum=0;
      for (int i = 0; i < size; i++) {
        array[i]=2;
        sum+=array[i];
      }      
    }
    return sum;
  }

  private int testVector(IntVector vector, int size, int round) {
    int sum = 0;
    IntVector.Accessor a = vector.getAccessor();
    IntVector.Mutator m = vector.getMutator();
    for (int j = 0; j < round; j++) {
      sum = 0;      
      for (int i = 0; i < size; i++) {
        m.set(i, 2);
        sum+=a.get(i);
      }
    }
    return sum;
  }
  
  private void evalVector(IntVector a, IntVector b, IntVector c, IntVector out, int size){
    IntVector.Accessor aa = a.getAccessor();
    IntVector.Accessor ba = b.getAccessor();
    IntVector.Accessor ca = c.getAccessor();
    IntVector.Accessor oa = out.getAccessor();
    IntVector.Mutator om = out.getMutator();
    
    long t0 = System.nanoTime();
    int n = 0;
    for (int i = 0; i < size; i++) {
      om.set(i, aa.get(i) + ba.get(i) - ca.get(i));
      n += oa.get(i);
    }
    long t1 = System.nanoTime();
    System.out.println("evalV\t"+n+"\t"+(t1-t0)/1000000+" ms");
  }
  
  
  private void evalArray(int[] a, int[] b, int[] c, int[] out){
    long t0 = System.nanoTime();
    int n = 0;
    for (int i = 0; i < a.length; i++) {
      out[i] = a[i] + b[i] - c[i];
      n += out[i];
    }
    long t1 = System.nanoTime();
    System.out.println("evalA\t"+n+"\t"+(t1-t0)/1000000 + " ms");
  }
  
  @Test
  public void testEval()throws Exception{
    int size = 50000000;
    int round = 100;
    BufferAllocator allocator = BufferAllocator.getAllocator(null);
    IntVector aVector = new IntVector(null, allocator);
    IntVector bVector = new IntVector(null, allocator);
    IntVector cVector = new IntVector(null, allocator);
    IntVector outVector = new IntVector(null, allocator);
    aVector.allocateNew(size);
    bVector.allocateNew(size);
    cVector.allocateNew(size);
    outVector.allocateNew(size);
    int[] aArray = new int[size];
    int[] bArray = new int[size];
    int[] cArray = new int[size];
    int[] outArray = new int[size];
    for (int i = 0; i < round; i++) {
      evalArray(aArray, bArray, cArray, outArray);
      evalVector(aVector, bVector, cVector, outVector, size);      
    }
  }
}
