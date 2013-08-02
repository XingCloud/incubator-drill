package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.ConstantValues;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.agg.AggregatingWrapperEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.agg.CountDistinctAggregator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;

import java.util.Arrays;
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

  private FragmentContext context;
  private PhysicalCollapsingAggregate config;
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


  public CollapsingAggregateBatch(FragmentContext context, PhysicalCollapsingAggregate config, RecordBatch incoming) {
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
    carryovers = new BasicEvaluator[config.getCarryovers().length];
    carryoverNames = new FieldReference[config.getCarryovers().length];
    carryOverTypes = new MajorType[carryovers.length];

    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i] = evaluatorFactory.
        getAggregateEvaluator(incoming, config.getAggregations()[i].getExpr());
      if (aggregatingEvaluators[i] instanceof CountDistinctAggregator) {
        BasicEvaluator within = boundaryKey != null ? boundaryKey : new ConstantValues.IntegerScalar(0, context);
        ((CountDistinctAggregator) aggregatingEvaluators[i]).setWithin(within);
      } else if (aggregatingEvaluators[i] instanceof AggregatingWrapperEvaluator) {
        CountDistinctAggregator countDistinctAggregator =
          ((AggregatingWrapperEvaluator) aggregatingEvaluators[i]).getCountDistinctAggregator();
        if (countDistinctAggregator != null) {
          BasicEvaluator within = boundaryKey != null ? boundaryKey : new ConstantValues.IntegerScalar(0, context);
          countDistinctAggregator.setWithin(within);
        }
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
      consumeCurrent();
      int groupId = 0;
      Object[] carryOverValue = null;
      Long[] aggValues = new Long[aggregatingEvaluators.length];
      for (int i = 0; i < aggregatingEvaluators.length; i++) {
        aggValues[i] = ((BigIntVector) aggregatingEvaluators[i].eval()).getAccessor().get(0);
      }

      if (carryovers.length != 0) {
        carryOverValue = new Object[carryovers.length];
        ValueVector v;
        for (int i = 0; i < carryovers.length; i++) {
          v = carryovers[i].eval();
          carryOverValue[i] = v.getAccessor().getObject(0);
          carryOverTypes[i] = v.getField().getType();
        }
      }

      if (boundaryKey != null) {
        groupId = ((IntVector) boundaryKey.eval()).getAccessor().get(0);
      }

      mergeAggValues(groupId, aggValues, carryOverValue);
      o = incoming.next();
    }

    writeOutPut();
    hasMore = false;
    return IterOutcome.OK_NEW_SCHEMA;
  }


  private void buildSchema() {
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

    buildSchema();

    for (MaterializedField f : materializedFieldList) {
      v = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(v, recordCount, 50);
      outputVectors.add(v);
    }


    int i = 0;
    for (AggValue aggValue : aggValues.values()) {
      // test
      System.out.println(aggValue);
      for (int j = 0; j < outColumnsLength; j++) {
        outputVectors.get(j).getMutator().setObject(i, aggValue.getObject(j));
      }
      i++;
    }

    for(ValueVector vector : outputVectors){
      vector.getMutator().setValueCount(recordCount);
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

    @Override
    public String toString() {
      return "AggValue{" +
        "aggValues=" + Arrays.toString(aggValues) +
        ", carryOvers=" + Arrays.toString(carryOvers) +
        '}' ;
    }

  }


}
