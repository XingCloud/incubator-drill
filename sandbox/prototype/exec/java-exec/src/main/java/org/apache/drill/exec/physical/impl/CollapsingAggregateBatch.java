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
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/11/13
 * Time: 1:53 PM
 */
public class CollapsingAggregateBatch extends BaseRecordBatch {

  final static Logger logger = LoggerFactory.getLogger(CollapsingAggregateBatch.class);

  private FragmentContext context;
  private CollapsingAggregatePOP config;
  private RecordBatch incoming;
  private BatchSchema batchSchema;
  private boolean hasMore = true;


  private AggregatingEvaluator[] aggregatingEvaluators;
  private SchemaPath[] aggNames;

  private BasicEvaluator[] carryovers;
  private SchemaPath[] carryoverNames;


  private SchemaPath boundaryPath[];
  private BasicEvaluator boundaryKey;

  private Map<Integer, AggValue> aggValues = new HashMap<>();
  private MajorType[] carryOverTypes;

  private int outColumnsLength;
  private List<MaterializedField> materializedFieldList = Lists.newArrayList();


  public CollapsingAggregateBatch(FragmentContext context, CollapsingAggregatePOP config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    setupEvals();

  }

  @Override
  public void setupEvals() {
    EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
    if (config.getWithin() != null) {
      boundaryPath = new SchemaPath[]{config.getWithin()};
      boundaryKey = evaluatorFactory.getBasicEvaluator(incoming, config.getWithin());
    }
    aggregatingEvaluators = new AggregatingEvaluator[config.getAggregations().length];
    aggNames = new SchemaPath[aggregatingEvaluators.length];
    if (config.getCarryovers() != null)
      carryovers = new BasicEvaluator[config.getCarryovers().length];
    else
      carryovers = new BasicEvaluator[0];
    carryoverNames = new FieldReference[carryovers.length];
    carryOverTypes = new MajorType[carryovers.length];
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i] = evaluatorFactory.
        getAggregateEvaluator(incoming, config.getAggregations()[i].getExpr());
      if (aggregatingEvaluators[i] instanceof CountDistinctAggregator) {
        boolean boundaryNeedClear = true;
        BasicEvaluator within;
        if (boundaryKey != null) {
          within = boundaryKey;
          boundaryNeedClear = false;
        } else {
          within = new ConstantValues.IntegerScalar(0, this);
        }
        ((CountDistinctAggregator) aggregatingEvaluators[i]).setWithin(within, boundaryNeedClear);
      }
      aggNames[i] = config.getAggregations()[i].getRef();
    }
    for (int i = 0; i < carryovers.length; i++) {
      carryovers[i] = evaluatorFactory.getBasicEvaluator(incoming, config.getCarryovers()[i]);
      carryoverNames[i] = config.getCarryovers()[i];
    }
    outColumnsLength = aggNames.length + carryoverNames.length;
  }

  private void consumeCurrent() {
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i].addBatch();
    }
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return batchSchema;
  }

  @Override
  public void kill() {
    releaseAssets();
    incoming.kill();
  }

  @Override
  public IterOutcome next() {
    if (!hasMore) {
      recordCount = 0;
      outputVectors.clear();
      return IterOutcome.NONE;
    }
    IterOutcome o = incoming.next();
    while (o != IterOutcome.NONE) {
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
        for (ValueVector v : incoming) {
          v.close();
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
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (int i = 0; i < aggNames.length; i++) {
      MaterializedField f = MaterializedField.create(aggNames[i], Types.required(MinorType.BIGINT));
      schemaBuilder.addField(f);
      materializedFieldList.add(f);
    }
    for (int i = 0; i < carryoverNames.length; i++) {
      MaterializedField f = MaterializedField.create(carryoverNames[i], carryOverTypes[i]);
      schemaBuilder.addField(f);
      materializedFieldList.add(f);
    }
    batchSchema = schemaBuilder.build();
  }

  private void writeOutPut() {
    ValueVector v;
    recordCount = aggValues.size();
    outputVectors.clear();
    setupSchema();
    for (MaterializedField f : materializedFieldList) {
      v = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(v, recordCount, 8);
      outputVectors.add(v);
    }
    int i = 0;
    for (AggValue aggValue : aggValues.values()) {
      for (int j = 0; j < outColumnsLength; j++) {
        outputVectors.get(j).getMutator().setObject(i, aggValue.getObject(j));
      }
      i++;
    }
    for (ValueVector vector : outputVectors) {
      vector.getMutator().setValueCount(recordCount);
    }
  }

  @Override
  public void releaseAssets() {
    for (ValueVector v : outputVectors) {
      v.close();
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
