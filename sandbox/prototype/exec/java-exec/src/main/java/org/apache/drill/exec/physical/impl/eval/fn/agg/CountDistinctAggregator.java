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
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:53 AM
 */

@FunctionEvaluator("count_distinct")
public class CountDistinctAggregator implements AggregatingEvaluator {


  private long l = 0;
  private BasicEvaluator child;
  private BasicEvaluator boundary;
  private RecordBatch recordBatch;
  private Map<Integer, Set<Object>> duplicates = new HashMap<>();
  private BigIntVector value;

  public CountDistinctAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.child = args.getOnlyEvaluator();
    this.recordBatch = recordBatch;
  }

  @Override
  public void addBatch() {
    ValueVector v = child.eval();
    Object o;
    Set<Object> duplicate;
    IntVector boundaryVector =  (IntVector) boundary.eval() ;
    int boundaryKey = boundaryVector.getAccessor().get(0);
    boundaryVector.close();
    duplicate = duplicates.get(boundaryKey);
    if (duplicate == null) {
      duplicate = new HashSet<>();
      duplicates.put(boundaryKey, duplicate);
    }
    for (int i = 0; i < v.getAccessor().getValueCount(); i++) {
      o = v.getAccessor().getObject(i);
      if (!duplicate.contains(o)) {
        l++;
        duplicate.add(o);
      }
    }
    v.close();
  }

  @Override
  public ValueVector eval() {
    if (value == null) {
      value = new BigIntVector(MaterializedField.create(new SchemaPath("count_distinct", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.BIGINT)),
        recordBatch.getContext().getAllocator());
    }
    value.allocateNew(1);
    value.getMutator().set(0, l);
    value.getMutator().setValueCount(1);
    l = 0;
    return value;
  }

  public void setWithin(BasicEvaluator boundary) {
    this.boundary = boundary;
  }
}
