package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.util.operation.Comparator;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/8/13
 * Time: 11:04 AM
 */
public class ComparisonEvaluators {

  private abstract static class ComparisonEvaluator extends BaseBasicEvaluator {
    protected final BasicEvaluator left;
    protected final BasicEvaluator right;

    public ComparisonEvaluator(RecordBatch recordBatch, FunctionArguments args) {
      super(args.isOnlyConstants(), recordBatch);
      this.left = args.getEvaluator(0);
      this.right = args.getEvaluator(1);

    }
  }

  @FunctionEvaluator("equal")
  public static class EqualEvaluator extends ComparisonEvaluator {
    public EqualEvaluator(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public ValueVector eval() {
      return Comparator.Equal(left.eval(), right.eval(),recordBatch.getContext().getAllocator());
    }
  }

  @FunctionEvaluator("greater than")
  public static class GreaterThan extends ComparisonEvaluator {

    public GreaterThan(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public ValueVector eval() {
      return Comparator.GreaterThan(left.eval(), right.eval(),recordBatch.getContext().getAllocator());
    }

  }

  @FunctionEvaluator("less than")
  public static class LessThan extends ComparisonEvaluator {

    public LessThan(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public ValueVector eval() {
      return Comparator.LessThan(left.eval(), right.eval(),recordBatch.getContext().getAllocator());
    }
  }

  @FunctionEvaluator("less than or equal to")
  public static class LessThanOrEqualTo extends ComparisonEvaluator {

    public LessThanOrEqualTo(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public ValueVector eval() {
      return Comparator.LessEqual(left.eval(), right.eval(),recordBatch.getContext().getAllocator());
    }
  }

  @FunctionEvaluator("greater than or equal to")
  public static class GreaterOrEqualTo extends ComparisonEvaluator {

    public GreaterOrEqualTo(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public ValueVector eval() {
      return Comparator.GreaterEqual(left.eval(), right.eval(),recordBatch.getContext().getAllocator());
    }
  }

  @FunctionEvaluator("and")
  public static class And extends BaseBasicEvaluator {
    private final BasicEvaluator left;
    private final BasicEvaluator right;
    private BitVector value;

    public And(RecordBatch recordBatch, FunctionArguments args) {

      super(args.isOnlyConstants(), recordBatch);
      this.left = args.getEvaluator(0);
      this.right = args.getEvaluator(1);
      value = new BitVector(MaterializedField.create(new SchemaPath("and", ExpressionPosition.UNKNOWN),
        Types.required(
          TypeProtos.MinorType.BOOLEAN)),
        recordBatch.getContext().getAllocator());
    }

    @Override
    public ValueVector eval() {
      BitVector.Accessor leftAccessor = ((BitVector) left.eval()).getAccessor();
      BitVector.Accessor rightAccessor = ((BitVector) right.eval()).getAccessor();

      value.allocateNew(leftAccessor.getValueCount());
      BitVector.Mutator valueMutator = value.getMutator();
      valueMutator.setValueCount(leftAccessor.getValueCount());
      for (int i = 0; i < leftAccessor.getValueCount(); i++) {

        if (leftAccessor.get(i) == 1 && rightAccessor.get(i) == 1) {
          valueMutator.set(i, 1);
        }
      }
      return value;
    }
  }


}
