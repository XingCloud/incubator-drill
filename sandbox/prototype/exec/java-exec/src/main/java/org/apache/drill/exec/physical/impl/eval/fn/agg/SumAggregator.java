package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:55 AM
 */

@FunctionEvaluator("sum")
public class SumAggregator implements AggregatingEvaluator {

  private BasicEvaluator child;
  private long l = 0;
  private RecordBatch recordBatch;
  private BigIntVector value;

  public SumAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.recordBatch = recordBatch;
    child = args.getOnlyEvaluator();

  }

  @Override
  public void addBatch() {

    BigIntVector bigIntVector = (BigIntVector) child.eval();
    BigIntVector.Accessor accessor = bigIntVector.getAccessor();
    int recordCount = accessor.getValueCount();
    int i;
    long sum = 0;

    for (i = 0; i < recordCount; i++) {
      sum += accessor.get(i);
    }
    l += sum;
  }


  @Override
  public ValueVector eval() {

    if (value == null) {
      value = new BigIntVector(MaterializedField.create(new SchemaPath("sum", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.BIGINT)),
        recordBatch.getContext().getAllocator());

    }

    value.allocateNew(1);
    value.getMutator().set(0, l);
    value.getMutator().setValueCount(1);
    l = 0;
    return value;
  }
}
