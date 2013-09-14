package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.*;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
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
    int i;
    RecordBatch recordBatch;

    public IntegerScalar(int i, RecordBatch recordBatch) {
      this.i = i;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (intVector == null) {
        intVector = new IntVector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.INT)), recordBatch.getContext().getAllocator());
      }
      intVector.allocateNew(1);
      intVector.getMutator().set(0, i);
      intVector.getMutator().setValueCount(1);
      return intVector;
    }
  }

  public static class LongScalar implements BasicEvaluator {

    BigIntVector bigIntVector;
    long l;
    RecordBatch recordBatch;

    public LongScalar(long l, RecordBatch recordBatch) {
      this.l = l;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (bigIntVector == null) {
        bigIntVector = new BigIntVector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.BIGINT)), recordBatch.getContext().getAllocator());

      }
      bigIntVector.allocateNew(1);
      bigIntVector.getMutator().set(0, l);
      bigIntVector.getMutator().setValueCount(1);
      return bigIntVector;
    }
  }

  public static class FolatScalar implements BasicEvaluator {
    Float4Vector float4Vector;
    float f;
    RecordBatch recordBatch;

    public FolatScalar(float f, RecordBatch recordBatch) {
      this.f = f;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (float4Vector == null) {
        float4Vector = new Float4Vector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.FLOAT4)), recordBatch.getContext().getAllocator());

      }
      float4Vector.allocateNew(1);
      float4Vector.getMutator().set(0, f);
      float4Vector.getMutator().setValueCount(1);
      return float4Vector;
    }
  }

  public static class DoubleScalar implements BasicEvaluator {
    Float8Vector float8Vector;
    double d;
    RecordBatch recordBatch;

    public DoubleScalar(double d, RecordBatch recordBatch) {
      this.d = d;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (float8Vector == null) {
        float8Vector = new Float8Vector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.FLOAT8)), recordBatch.getContext().getAllocator());

      }
      float8Vector.allocateNew(1);
      float8Vector.getMutator().set(0, d);
      float8Vector.getMutator().setValueCount(1);
      return float8Vector;
    }
  }

  public static class StringScalar implements BasicEvaluator {
    VarCharVector varCharVector;
    CharSequence seq;
    RecordBatch recordBatch;

    public StringScalar(CharSequence seq, RecordBatch recordBatch) {
      this.seq = seq;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (varCharVector == null) {
        varCharVector = new VarCharVector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.VARCHAR)), recordBatch.getContext().getAllocator());

      }
      varCharVector.allocateNew(seq.length() * 2 + 8, 1);
      varCharVector.getMutator().set(0, seq.toString().getBytes());
      varCharVector.getMutator().setValueCount(1);
      return varCharVector;
    }
  }

  public static class BooleanScalar implements BasicEvaluator {

    BitVector bitVector;
    boolean b;
    RecordBatch recordBatch;

    public BooleanScalar(boolean b, RecordBatch recordBatch) {
      this.b = b;
      this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
      if (bitVector == null) {
        bitVector = new BitVector(MaterializedField.create(new SchemaPath("constant", ExpressionPosition.UNKNOWN), Types.required(MinorType.BIT)),
          recordBatch.getContext().getAllocator());

      }
      bitVector.allocateNew(1);
      bitVector.getMutator().set(0, 1);
      bitVector.getMutator().setValueCount(1);
      return bitVector;
    }
  }
}
