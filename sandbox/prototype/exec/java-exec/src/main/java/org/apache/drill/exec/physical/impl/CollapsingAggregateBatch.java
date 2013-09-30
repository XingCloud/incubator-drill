package org.apache.drill.exec.physical.impl;

import com.google.common.collect.Maps;
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
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private BasicEvaluator boundaryKey = new ConstantValues.IntegerScalar(0, this);
  private BasicEvaluator[] carryOversEvaluator = new BasicEvaluator[0];
  private Map<Integer, Object[]> carryOversValues = Maps.newHashMap();
  private MajorType[] carryOverTypes = new MajorType[0];
  private boolean newSchema = false;

  public CollapsingAggregateBatch(FragmentContext context, CollapsingAggregatePOP config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    setupEvals();
  }

  @Override
  public void setupEvals() {
    EvaluatorFactory ef = new BasicEvaluatorFactory();
    if (config.getWithin() != null) {
      boundaryKey = ef.getBasicEvaluator(incoming, config.getWithin());
    }
    aggregatingEvaluators = new AggregatingEvaluator[config.getAggregations().length];
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      aggregatingEvaluators[i] = ef.
        getAggregateEvaluator(incoming, config.getAggregations()[i].getExpr());
      aggregatingEvaluators[i].setWithin(boundaryKey);
    }
    if (config.getCarryovers() != null) {
      carryOversEvaluator = new BasicEvaluator[config.getCarryovers().length];
      carryOverTypes = new MajorType[carryOversEvaluator.length];
      for (int i = 0; i < carryOversEvaluator.length; i++) {
        carryOversEvaluator[i] = ef.getBasicEvaluator(incoming, config.getCarryovers()[i]);
      }
    }
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
      return IterOutcome.NONE;
    }
    IterOutcome o = incoming.next();
    switch (o) {
      case OK_NEW_SCHEMA:
        newSchema = true;
      case OK:
        try {
          doAggerate();
        } catch (Exception e) {
          logger.error(e.getMessage());
          e.printStackTrace();
          context.fail(e);
          return IterOutcome.STOP;
        }
      case NOT_YET:
        return IterOutcome.NOT_YET;
      case STOP:
        return IterOutcome.STOP;
      case NONE:
        if (!newSchema) {
          if (carryOversEvaluator.length == 0) {
            upstreamZero();
          } else {
            return IterOutcome.NONE;
          }
        } else {
          upstream();
        }
        setupSchema();
        hasMore = false;
        return IterOutcome.OK_NEW_SCHEMA;
    }
    return IterOutcome.NONE;
  }

  public void doAggerate() {
    consumeCurrent();
    Object[] carryOverValue = new Object[carryOversEvaluator.length];
    for (int i = 0; i < carryOversEvaluator.length; i++) {
      ValueVector v = carryOversEvaluator[i].eval();
      carryOverValue[i] = v.getAccessor().getObject(0);
      carryOverTypes[i] = v.getField().getType();
      v.close();
    }
    IntVector boundaryVector = (IntVector) boundaryKey.eval();
    int boundaryKey = boundaryVector.getAccessor().get(0);
    boundaryVector.close();
    carryOversValues.put(boundaryKey, carryOverValue);
    for (ValueVector v : incoming) {
      v.close();
    }
  }


  private void setupSchema() {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : outputVectors) {
      schemaBuilder.addField(v.getField());
    }
    batchSchema = schemaBuilder.build();
  }

  private void upstream() {
    outputVectors.clear();
    upstreamAggValues();
    upstreamCarryOvers();
    for (ValueVector vector : outputVectors) {
      vector.getMutator().setValueCount(recordCount);
    }
  }

  private void upstreamAggValues() {
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      MaterializedField f = MaterializedField.create(config.getAggregations()[i].getRef(), Types.required(MinorType.BIGINT));
      ValueVector in = aggregatingEvaluators[i].eval();
      ValueVector out = TransferHelper.transferVector(in);
      recordCount = out.getAccessor().getValueCount();
      out.setField(f);
      outputVectors.add(out);
    }
  }


  private void upstreamCarryOvers() {
    for (int i = 0; i < carryOversEvaluator.length; i++) {
      MaterializedField f = MaterializedField.create(config.getCarryovers()[i], carryOverTypes[i]);
      ValueVector v = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(v, recordCount, 4);
      ValueVector.Mutator m = v.getMutator();
      for (int j = 0; j < recordCount; j++) {
        m.setObject(j, carryOversValues.get(j)[i]);
      }
      m.setValueCount(recordCount);
      outputVectors.add(v);
    }
  }

  private void upstreamZero() {
    for (int i = 0; i < aggregatingEvaluators.length; i++) {
      MaterializedField f = MaterializedField.create(config.getAggregations()[i].getRef(), Types.required(MinorType.BIGINT));
      BigIntVector out = new BigIntVector(f, context.getAllocator());
      out.allocateNew(1);
      out.getMutator().set(0, 0);
      out.getMutator().setValueCount(1);
      outputVectors.add(out);
      recordCount = 1;
    }
  }

  @Override
  public void releaseAssets() {
    for (ValueVector v : outputVectors) {
      v.close();
    }
  }


}
