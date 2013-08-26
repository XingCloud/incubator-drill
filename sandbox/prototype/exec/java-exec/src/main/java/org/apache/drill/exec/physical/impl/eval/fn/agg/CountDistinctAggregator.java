package org.apache.drill.exec.physical.impl.eval.fn.agg;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.collect.Maps;
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

import java.util.Map;

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
  private Map<Integer, DistinctCounter> distinctCounters = Maps.newHashMap();
  private BigIntVector value;

  public CountDistinctAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.child = args.getOnlyEvaluator();
    this.recordBatch = recordBatch;
  }

  @Override
  public void addBatch() {
    ValueVector v = child.eval();
    DistinctCounter distinctCounter;
    IntVector boundaryVector = (IntVector) boundary.eval();
    int boundaryKey = boundaryVector.getAccessor().get(0);
    boundaryVector.close();
    distinctCounter = distinctCounters.get(boundaryKey);
    if (distinctCounter == null) {
      distinctCounter = getDefaultDistinctCounter();
      distinctCounters.put(boundaryKey, distinctCounter);
    } else if (distinctCounter.needUpgrade()) {
      distinctCounter = getHyperDistinctCounter((SetCounter) distinctCounter);
      distinctCounters.put(boundaryKey, distinctCounter);
    }
    distinctCounter.add(v);
    l = distinctCounter.gap();
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


  private DistinctCounter getDefaultDistinctCounter() {
    return new SetCounter();
  }

  private DistinctCounter getHyperDistinctCounter() {
    return new HyperCounter();
  }

  private DistinctCounter getHyperDistinctCounter(SetCounter setCounter) {
    return new HyperCounter(setCounter);
  }

  abstract class DistinctCounter {
    long previous;
    long current;

    public abstract void add(ValueVector v);

    public long size() {
      return current;
    }

    public long gap() {
      return current - previous;
    }

    public boolean needUpgrade() {
      return false;
    }
  }

  class SetCounter extends DistinctCounter {
    ObjectOpenHashSet<Object> set = new ObjectOpenHashSet<>();

    @Override
    public void add(ValueVector v) {
      ValueVector.Accessor a = v.getAccessor();
      for (int i = 0; i < a.getValueCount(); i++) {
        set.add(a.getObject(i));
      }
      previous = current;
      current = set.size();
    }

    @Override
    public boolean needUpgrade() {
      if (current > 8 * 1024) {
        return true;
      }
      return false;
    }
  }

  class HyperCounter extends DistinctCounter {
    HyperLogLog hyperLogLog = new HyperLogLog(16);

    public HyperCounter() {}

    public HyperCounter(SetCounter setCounter) {
      for (ObjectCursor<Object> o : setCounter.set) {
        hyperLogLog.offer(o.value);
      }
      previous = setCounter.previous;
      current = setCounter.current;
    }

    @Override
    public void add(ValueVector v) {
      ValueVector.Accessor a = v.getAccessor();
      for (int i = 0; i < a.getValueCount(); i++) {
        hyperLogLog.offer(a.getObject(i));
      }
      previous = current;
      current = hyperLogLog.cardinality();
    }
  }
}
