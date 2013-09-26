package org.apache.drill.exec.util.operation;

import org.apache.drill.common.exceptions.DrillRuntimeException;
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
    if (left.getAccessor().getValueCount() > 1) {
      if (right.getAccessor().getValueCount() > 1) {
        return V2V(left, right, allocator);
      }
      return V2O(left, right, allocator);
    } else {
      if (right.getAccessor().getValueCount() > 1) {
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
    IntVector v = new IntVector(left.getField(), allocator);
    int recordCount = right.getAccessor().getValueCount();
    v.allocateNew(recordCount);
    IntVector.Mutator mutator = v.getMutator();

    if (left instanceof IntVector && right instanceof IntVector) {
      IntVector.Accessor leftAccessor = ((IntVector) left).getAccessor();
      IntVector.Accessor rightAccessor = ((IntVector) right).getAccessor();
      int leftValue, rightValue;
      for (int i = 0; i < recordCount; i++) {
        leftValue = leftAccessor.get(i);
        rightValue = rightAccessor.get(i);
        mutator.set(i, leftValue == rightValue ? 0 : leftValue > rightValue ? 1 : -1);
      }
      mutator.setValueCount(recordCount);
    } else if (left instanceof VarCharVector && right instanceof VarCharVector) {
      VarCharVector.Accessor leftAccessor = ((VarCharVector) left).getAccessor();
      VarCharVector.Accessor rightAccessor = ((VarCharVector) right).getAccessor();
      int compareValue;
      for (int i = 0; i < recordCount; i++) {
        compareValue = new String(leftAccessor.get(i)).compareTo(new String(rightAccessor.get(i)));
        mutator.set(i, compareValue == 0 ? 0 : compareValue > 0 ? 1 : -1);
      }
      mutator.setValueCount(recordCount);
    } else {
      throw new DrillRuntimeException("V2V not support between  " + left.getClass() + ":" + right.getClass());
    }
    return v;
  }

  private static IntVector O2V(ValueVector left, ValueVector right, BufferAllocator allocator) {
    IntVector v = new IntVector(right.getField(), allocator);

    int recordCount = right.getAccessor().getValueCount();
    v.allocateNew(recordCount);
    IntVector.Mutator mutator = v.getMutator();

    int j = 0;
    byte b = 0;
    long leftValue = 0;

    if (left instanceof FixedWidthVector) {
      if (left instanceof IntVector) {
        leftValue = ((IntVector) left).getAccessor().get(0);
      } else if (left instanceof BigIntVector) {
        leftValue = ((BigIntVector) left).getAccessor().get(0);
      }
      if (right instanceof IntVector) {
        IntVector.Accessor ints = ((IntVector) right).getAccessor();
        for (j = 0; j < recordCount; j++) {
          int rightValue = ints.get(j);
          b = (byte) (leftValue == rightValue ? 0 : leftValue > rightValue ? 1 : -1);
          mutator.set(j, b);
        }
      } else if (right instanceof BigIntVector) {
        BigIntVector.Accessor longs = ((BigIntVector) right).getAccessor();
        long i = 0;
        for (j = 0; j < recordCount; j++) {
          long rightValue = longs.get(j);
          b = (byte) (leftValue == rightValue ? 0 : leftValue > rightValue ? 1 : -1);
          mutator.set(j, b);
        }
      }
    } else if (left instanceof VarCharVector) {
      String leftString = new String(((VarCharVector) left).getAccessor().get(0));
      VarCharVector.Accessor strs = ((VarCharVector) right).getAccessor();
      int i;
      for (j = 0; j < recordCount; j++) {
        i = leftString.compareTo(new String(strs.get(j)));
        i = i == 0 ? 0 : i > 0 ? 1 : -1;
        mutator.set(j, i);
      }
    }
    mutator.setValueCount(recordCount);
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
    intVector.close();
    bitMutator.setValueCount(recordCount);
    return bits;
  }
}
