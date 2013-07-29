package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:49 AM
 */

@FunctionEvaluator("count")
public class CountAggregator implements AggregatingEvaluator {

  private long l = 0l;
  private BasicEvaluator child;
  private RecordBatch recordBatch;
  private BigIntVector value;


  public CountAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.recordBatch = recordBatch;
    child = args.getOnlyEvaluator();
    value = new BigIntVector(MaterializedField.create(
      new SchemaPath("count", ExpressionPosition.UNKNOWN),
      Types.required(MinorType.BIGINT)), recordBatch.getContext().getAllocator());

  }

  @Override
  public void addBatch() {

    l += child.eval().getAccessor().getValueCount();
  }

  @Override
  public BigIntVector eval() {
    value.allocateNew(1);
    value.getMutator().setValueCount(1);
    value.getMutator().set(0, l);
    l = 0;
    return value;
  }
}
