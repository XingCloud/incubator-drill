package org.apache.drill.exec.physical.impl.eval.fn.agg;

import com.google.common.collect.Maps;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
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
 * Time: 9:49 AM
 */

@FunctionEvaluator("count")
public class CountAggregator extends AbstractAggregatingEvaluator {

  private Map<Integer, Long> counts = Maps.newHashMap();

  public CountAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.recordBatch = recordBatch;
    child = args.getOnlyEvaluator();
  }

  @Override
  public void addBatch() {
    ValueVector v = child.eval();
    int boundaryKey = getBoundary();
    int count = v.getAccessor().getValueCount();
    Long l = counts.get(boundaryKey);
    if (l == null) {
      counts.put(boundaryKey, (long) count);
    } else {
      counts.put(boundaryKey, l + count);
    }
    v.close();
  }

  @Override
  public BigIntVector eval() {
    value = new BigIntVector(MaterializedField.create(
      new SchemaPath("count", ExpressionPosition.UNKNOWN),
      Types.required(MinorType.BIGINT)), recordBatch.getContext().getAllocator());
    int recordCount = counts.size();
    value.allocateNew(recordCount);
    BigIntVector.Mutator m = value.getMutator();
    for (int i = 0; i < recordCount; i++) {
      m.set(i, counts.get(i));
    }
    m.setValueCount(recordCount);
    counts = null ;
    return value;
  }
}
