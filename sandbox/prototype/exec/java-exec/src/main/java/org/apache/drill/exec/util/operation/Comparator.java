package org.apache.drill.exec.util.operation;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.vector.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 10:53 PM
 */
public class Comparator {

  private final static byte POSITIVE = 1;
  private final static byte ZERO = 0;
  private final static byte NEGATIVE = -1;

  enum RELATION {
    EQUAL_TO, GREATER_THAN, LESS_THAN,
  }

  public static BitVector Equal(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return IntsToBits(Compare(left, right, allocator), RELATION.EQUAL_TO, ZERO, allocator);
  }

  public static BitVector GreaterThan(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return IntsToBits(Compare(left, right, allocator), RELATION.EQUAL_TO, POSITIVE, allocator);
  }

  public static BitVector LessThan(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return IntsToBits(Compare(left, right, allocator), RELATION.EQUAL_TO, NEGATIVE, allocator);
  }

  public static BitVector GreaterEqual(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return IntsToBits(Compare(left, right, allocator), RELATION.GREATER_THAN, NEGATIVE, allocator);
  }

  public static BitVector LessEqual(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return IntsToBits(Compare(left, right, allocator), RELATION.LESS_THAN, POSITIVE, allocator);
  }

  private static IntVector Compare(ValueVector left, ValueVector right, BufferAllocator allocator) {
    if (left.getValueCapacity() > 1) {
      if (right.getValueCapacity() > 1) {
        return V2V(left, right, allocator);
      }
      return V2O(left, right, allocator);
    } else {
      if (right.getValueCapacity() > 1) {
        return O2V(left, right, allocator);
      }
      return O2O(left, right, allocator);
    }

  }

  private static IntVector V2O(ValueVector left, ValueVector right, BufferAllocator allocator) {
    IntVector v = O2V(right, left, allocator);
    IntVector.Mutator mutator = v.getMutator();
    IntVector.Accessor accessor = v.getAccessor();
    for (int i = 0; i < accessor.getValueCount(); i++) {
      mutator.set(i, -accessor.get(i));
    }
    return v;
  }

  private static IntVector V2V(ValueVector left, ValueVector right, BufferAllocator allocator) {
    return null;
  }

  private static IntVector O2V(ValueVector left, ValueVector right, BufferAllocator allocator) {
    IntVector v = new IntVector(right.getField(), allocator);

    int recordCount = right.getAccessor().getValueCount();
    v.allocateNew(recordCount);
    IntVector.Mutator mutator = v.getMutator();
    mutator.setValueCount(recordCount);
    int j = 0;
    byte b = 0;
    if (left instanceof IntVector) {
      long l = ((IntVector) left).getAccessor().get(0);
      IntVector.Accessor ints = ((IntVector) right).getAccessor();
      int i = 0;
      for (j = 0; j < recordCount; j++) {
        i = ints.get(j);
        b = (byte) (l == i ? 0 : l > i ? 1 : -1);
        mutator.set(j, b);
      }
    } else if (left instanceof BigIntVector) {
      long l = ((BigIntVector) left).getAccessor().get(0);
      BigIntVector.Accessor longs = ((BigIntVector) right).getAccessor();
      long i = 0;
      for (j = 0; j < recordCount; j++) {
        i = longs.get(j);
        b = (byte) (l == i ? 0 : l > i ? 1 : -1);
        mutator.set(j, b);
      }
    } else if (left instanceof VarChar4Vector) {
      String leftString = new String(((VarChar4Vector) left).getAccessor().get(0));
      VarChar4Vector.Accessor strs = ((VarChar4Vector) right).getAccessor();
      for (j = 0; j < recordCount; j++) {
        mutator.set(j, (byte) leftString.compareTo(new String(strs.get(j))));
      }
    }

    return v;
  }

  private static IntVector O2O(ValueVector left, ValueVector right, BufferAllocator allocator) {

    IntVector intVector = new IntVector(null, allocator);
    intVector.allocateNew(1);
    intVector.getMutator().setValueCount(1);
    // TODO
    return intVector;
  }

  private static BitVector IntsToBits(IntVector intVector, RELATION relation, byte value, BufferAllocator allocator) {
    IntVector.Accessor intAccessor = intVector.getAccessor();
    int recordCount = intAccessor.getValueCount();
    BitVector bits = new BitVector(null, allocator);
    bits.allocateNew(recordCount);
    BitVector.Mutator bitMutator = bits.getMutator();
    bitMutator.setValueCount(recordCount);
    int b;
    for (int i = 0; i < recordCount; i++) {
      b = intAccessor.get(i);
      switch (relation) {
        case EQUAL_TO:
          if (b == value) bitMutator.set(i, 1);
          break;
        case GREATER_THAN:
          if (b > value) bitMutator.set(i, 1);
          break;
        case LESS_THAN:
          if (b < value) bitMutator.set(i, 1);
      }
    }
    return bits;
  }
}
