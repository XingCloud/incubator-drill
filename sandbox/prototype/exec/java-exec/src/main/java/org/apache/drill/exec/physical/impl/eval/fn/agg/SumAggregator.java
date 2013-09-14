package org.apache.drill.exec.physical.impl.eval.fn.agg;

import com.google.common.collect.Maps;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AbstractAggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:55 AM
 */

@FunctionEvaluator("sum")
public class SumAggregator extends AbstractAggregatingEvaluator {

  private Map<Integer, Long> sums = Maps.newHashMap();

  public SumAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.recordBatch = recordBatch;
    child = args.getOnlyEvaluator();
  }

  @Override
  public void addBatch() {
    BigIntVector bigIntVector = (BigIntVector) child.eval();
    BigIntVector.Accessor accessor = bigIntVector.getAccessor();
    int recordCount = accessor.getValueCount();
    long sum = 0;
    for (int i = 0; i < recordCount; i++) {
      sum += accessor.get(i);
    }
    int boundaryKey = getBoundary();
    Long l = sums.get(boundaryKey);
    if (l == null) {
      sums.put(boundaryKey, (long) sum);
    } else {
      sums.put(boundaryKey, l + sum);
    }
    bigIntVector.close();
  }


  @Override
  public ValueVector eval() {
    value = new BigIntVector(MaterializedField.create(new SchemaPath("sum", ExpressionPosition.UNKNOWN),
      Types.required(TypeProtos.MinorType.BIGINT)),
      recordBatch.getContext().getAllocator());
    int recordCount = sums.size();
    value.allocateNew(recordCount);
    BigIntVector.Mutator m = value.getMutator();
    for (int i = 0; i < recordCount; i++) {
      m.set(i, sums.get(i));
    }
    m.setValueCount(recordCount);
    return value;
  }
}
