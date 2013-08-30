package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.CollapsingAggregatePOP;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.ConstantValues;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.agg.CountDistinctAggregator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/11/13
 * Time: 1:53 PM
 */
public class CollapsingAggregateBatch extends AbstractRecordBatch<CollapsingAggregatePOP> {

  final static Logger logger = LoggerFactory.getLogger(CollapsingAggregateBatch.class);

  private RecordBatch incoming;
  private boolean hasMore = true;

  int recordCount = 0;


  private AggregatingEvaluator[] aggregatingEvaluators;
  private SchemaPath[] aggNames;

  private BasicEvaluator[] carryovers;
  private SchemaPath[] carryoverNames;


  private SchemaPath boundaryPath[];
  private BasicEvaluator boundaryKey;

  private Map<Integer, AggValue> aggValues = new HashMap<>();
  private MajorType[] carryOverTypes;

  private int outColumnsLength;
//  private List<MaterializedField> materializedFieldList = Lists.newArrayList();


  public CollapsingAggregateBatch(FragmentContext context, CollapsingAggregatePOP config, RecordBatch incoming) {
    super(config, context);
    this.incoming = incoming;
    setupEvals();

  }

  public void setupEvals() {
    EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
    if (popConfig.getWithin() != null) {
      boundaryPath = new SchemaPath[]{popConfig.getWithin()};
      boundaryKey = evaluatorFactory.getBasicEvaluator(incoming, popConfig.getWithin());
    }
    aggregatingEvaluators = new AggregatingEvaluator[popConfig.getAggregations().length];
    aggNames = new SchemaPath[aggregatingEvaluators.length];
    if (popConfig.getCarryovers() != null)
      carryovers = new BasicEvaluator[popConfig.getCarryovers().length];
    else
      carryovers = new BasicEvaluator[0];
    carryoverNames = new FieldReference[carryovers.length];
    carryOverTypes = new MajorType[carryovers.length];
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i] = evaluatorFactory.
        getAggregateEvaluator(incoming, popConfig.getAggregations()[i].getExpr());
      if (aggregatingEvaluators[i] instanceof CountDistinctAggregator) {
        BasicEvaluator within = boundaryKey == null ? new ConstantValues.IntegerScalar(0, this) : boundaryKey;
        ((CountDistinctAggregator) aggregatingEvaluators[i]).setWithin(within);
      }
      aggNames[i] = popConfig.getAggregations()[i].getRef();
    }
    for (int i = 0; i < carryovers.length; i++) {
      carryovers[i] = evaluatorFactory.getBasicEvaluator(incoming, popConfig.getCarryovers()[i]);
      carryoverNames[i] = popConfig.getCarryovers()[i];
    }
    outColumnsLength = aggNames.length + carryoverNames.length;
  }

  private void consumeCurrent() {
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i].addBatch();
    }
  }


  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  protected void cleanup() {
    container.clear();
    super.cleanup();    //TODO method implementation
  }

  @Override
  public IterOutcome next() {
    if (!hasMore) {
      container.clear();
      return IterOutcome.NONE;
    }
    IterOutcome o = incoming.next();
    while (true) {
      if(o == IterOutcome.NONE)
        break;
      else if(o == IterOutcome.STOP){
        return  o ;
      }
      try {
        consumeCurrent();
        int groupId = 0;
        Object[] carryOverValue = null;
        Long[] aggValues = new Long[aggregatingEvaluators.length];
        for (int i = 0; i < aggregatingEvaluators.length; i++) {
          BigIntVector aggVector = (BigIntVector) aggregatingEvaluators[i].eval();
          aggValues[i] = aggVector.getAccessor().get(0);
          aggVector.close();
        }
        if (carryovers.length != 0) {
          carryOverValue = new Object[carryovers.length];
          ValueVector v;
          for (int i = 0; i < carryovers.length; i++) {
            v = carryovers[i].eval();
            carryOverValue[i] = v.getAccessor().getObject(0);
            carryOverTypes[i] = v.getField().getType();
            v.close();
          }
        }
        if (boundaryKey != null) {
          IntVector boundaryVector = (IntVector) boundaryKey.eval();
          groupId = boundaryVector.getAccessor().get(0);
          boundaryVector.close();
        }
        mergeAggValues(groupId, aggValues, carryOverValue);
        for (VectorWrapper<?> v : incoming) {
          v.release();
        }
        o = incoming.next();
      } catch (Exception e) {
        logger.error(e.getMessage());
        e.printStackTrace();
        context.fail(e);
        return IterOutcome.STOP;
      }
    }
    if (aggValues.isEmpty()) {
      return IterOutcome.NONE;
    }
    writeOutPut();
    hasMore = false;
    return IterOutcome.OK_NEW_SCHEMA;
  }


  private void setupSchema() {
    for (int i = 0; i < aggNames.length; i++) {
      MaterializedField f = MaterializedField.create(aggNames[i], Types.required(MinorType.BIGINT));
      ValueVector v = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(v, recordCount, 8);
      container.add(v);
    }
    for (int i = 0; i < carryoverNames.length; i++) {
      MaterializedField f = MaterializedField.create(carryoverNames[i], carryOverTypes[i]);
      ValueVector v = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(v, recordCount, 8);
      container.add(v);
    }
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  private void writeOutPut() {
    VectorWrapper<?> v;
    recordCount = aggValues.size();
    container.clear();
    setupSchema();
   
    int i = 0;
    for (AggValue aggValue : aggValues.values()) {
      Iterator<VectorWrapper<?>> vvs = container.iterator();
      for (int j = 0; j < outColumnsLength && vvs.hasNext(); j++) {
        v = vvs.next();
        v.getValueVector().getMutator().setObject(i, aggValue.getObject(j));
      }
      i++;
    }
    for (VectorWrapper<?> vector : container) {
      vector.getValueVector().getMutator().setValueCount(recordCount);
    }
  }

  private void mergeAggValues(int groupId, Long[] values, Object[] carryOvers) {
    AggValue val = aggValues.get(groupId);
    if (val == null) {
      aggValues.put(groupId, new AggValue(values, carryOvers));
    } else {
      Long[] aggVals = val.aggValues;
      for (int i = 0; i < aggVals.length; i++) {
        aggVals[i] += values[i];
      }
    }
  }

  class AggValue {
    Long[] aggValues;
    Object[] carryOvers;

    AggValue(Long[] aggValues, Object[] carryOvers) {
      this.aggValues = aggValues;
      this.carryOvers = carryOvers;
    }

    Object getObject(int i) {
      if (i < aggValues.length) {
        return aggValues[i];
      } else {
        return carryOvers[i - aggValues.length];
      }
    }
  }


}
