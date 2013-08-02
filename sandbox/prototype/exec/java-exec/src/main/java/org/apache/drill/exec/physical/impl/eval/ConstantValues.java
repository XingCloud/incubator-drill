package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.vector.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:30 PM
 */
public class ConstantValues {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConstantValues.class);


  public static class IntegerScalar implements BasicEvaluator {
    IntVector intVector;

    public IntegerScalar(int i, FragmentContext context) {
      intVector = new IntVector(null, context.getAllocator());
      intVector.allocateNew(1);
      intVector.getMutator().set(0, i);
      intVector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return intVector;
    }
  }

  public static class LongScalar implements BasicEvaluator {

    BigIntVector bigIntVector;

    public LongScalar(long l, FragmentContext context) {
      bigIntVector = new BigIntVector(null, context.getAllocator());
      bigIntVector.allocateNew(1);
      bigIntVector.getMutator().set(0, l);
      bigIntVector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return bigIntVector;
    }
  }

  public static class FolatScalar implements BasicEvaluator {
    Float4Vector float4Vector;

    public FolatScalar(float f, FragmentContext context) {
      float4Vector = new Float4Vector(null, context.getAllocator());
      float4Vector.allocateNew(1);
      float4Vector.getMutator().set(0, f);
      float4Vector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return float4Vector;
    }
  }

  public static class DoubleScalar implements BasicEvaluator {
    Float8Vector float8Vector;

    public DoubleScalar(double d, FragmentContext context) {
      float8Vector = new Float8Vector(null, context.getAllocator());
      float8Vector.allocateNew(1);
      float8Vector.getMutator().set(0, d);
      float8Vector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return float8Vector;
    }
  }

  public static class StringScalar implements BasicEvaluator {
    VarCharVector varCharVector;

    public StringScalar(CharSequence seq, FragmentContext context) {
      varCharVector = new VarCharVector(null, context.getAllocator());
      varCharVector.allocateNew(seq.length() * 2 + 8, 1);
      varCharVector.getMutator().set(0, seq.toString().getBytes());
      varCharVector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return varCharVector;
    }
  }

  public static class BooleanScalar implements BasicEvaluator {

    BitVector bitVector;

    public BooleanScalar(boolean b, FragmentContext context) {
      bitVector = new BitVector(null, context.getAllocator());
      bitVector.allocateNew(1);
      bitVector.getMutator().set(0, 1);
      bitVector.getMutator().setValueCount(1);
    }

    @Override
    public ValueVector eval() {
      return bitVector;
    }
  }
}
