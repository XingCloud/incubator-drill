package org.apache.drill.exec.physical.impl.eval.fn.agg;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
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
public class CountDistinctAggregator extends AbstractAggregatingEvaluator {

  private Map<Integer, DistinctCounter> distinctCounters = Maps.newHashMap();

  public CountDistinctAggregator(RecordBatch recordBatch, FunctionArguments args) {
    this.child = args.getOnlyEvaluator();
    this.recordBatch = recordBatch;
  }

  @Override
  public void addBatch() {
    ValueVector v = child.eval();
    DistinctCounter distinctCounter;
    int boundaryKey = getBoundary();
    distinctCounter = distinctCounters.get(boundaryKey);
    if (distinctCounter == null) {
      distinctCounter = getDefaultDistinctCounter();
      distinctCounters.put(boundaryKey, distinctCounter);
    } else if (distinctCounter.needUpgrade()) {
      distinctCounter = getHyperDistinctCounter((SetCounter) distinctCounter);
      distinctCounters.put(boundaryKey, distinctCounter);
    }
    distinctCounter.add(v);
    v.close();
  }

  @Override
  public ValueVector eval() {
    value = new BigIntVector(MaterializedField.create(new SchemaPath("count_distinct", ExpressionPosition.UNKNOWN),
      Types.required(TypeProtos.MinorType.BIGINT)),
      recordBatch.getContext().getAllocator());
    int recordCount = distinctCounters.size();
    value.allocateNew(recordCount);
    BigIntVector.Mutator m = value.getMutator();
    for (int i = 0; i < recordCount; i++) {
      m.set(i, distinctCounters.get(i).cardinality());
    }
    m.setValueCount(recordCount);
    return value;
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

    public abstract void add(ValueVector v);

    public abstract long cardinality();

    public boolean needUpgrade() {
      return false;
    }
  }

  class SetCounter extends DistinctCounter {
    IntOpenHashSet set = new IntOpenHashSet();

    @Override
    public void add(ValueVector v) {
      IntVector.Accessor a = ((IntVector) v).getAccessor();
      for (int i = 0; i < a.getValueCount(); i++) {
        set.add(a.get(i));
      }
    }

    @Override
    public long cardinality() {
      return set.size();
    }

    @Override
    public boolean needUpgrade() {
      if (set.size() > 8 * 1024) {
        return true;
      }
      return false;
    }
  }

  class HyperCounter extends DistinctCounter {
    HyperLogLog hyperLogLog = new HyperLogLog(16);

    public HyperCounter() {
    }

    public HyperCounter(SetCounter setCounter) {
      for (IntCursor o : setCounter.set) {
        hyperLogLog.offer(o.value);
      }
    }

    @Override
    public void add(ValueVector v) {
      ValueVector.Accessor a = v.getAccessor();
      for (int i = 0; i < a.getValueCount(); i++) {
        hyperLogLog.offer(a.getObject(i));
      }
    }

    @Override
    public long cardinality() {
      return hyperLogLog.cardinality();
    }
  }
}
