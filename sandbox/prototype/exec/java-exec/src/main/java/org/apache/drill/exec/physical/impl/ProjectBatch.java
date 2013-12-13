package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.TransferHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/2/13
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class ProjectBatch extends BaseRecordBatch {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Project.class);

  private FragmentContext context;
  private Project config;
  private RecordBatch incoming;
  private List<NamedExpression> selections;
  private BasicEvaluator evaluators[];
  private String paths[];


  private BatchSchema batchSchema;
  private boolean new_schema = true;
  private SchemaBuilder schemaBuilder;


  public ProjectBatch(FragmentContext context, Project config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    this.selections = config.getExprs();
    this.paths = new String[selections.size()];
    this.evaluators = new BasicEvaluator[selections.size()];
    setupEvals();
  }

  @Override
  public void setupEvals() {
    EvaluatorFactory builder = new BasicEvaluatorFactory();
    for (int i = 0; i < selections.size(); i++) {
      paths[i] = selections.get(i).getRef().getPath().toString();
      evaluators[i] = builder.getBasicEvaluator(incoming, selections.get(i).getExpr());
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
    IterOutcome o = incoming.next();
    switch (o) {
      case NONE:
      case STOP:
      case NOT_YET:
        recordCount = 0;
        break;
      case OK_NEW_SCHEMA:
        new_schema = true;
        schemaBuilder = BatchSchema.newBuilder();
      case OK:
        try {
          outputVectors.clear();
          recordCount = incoming.getRecordCount();
          for (int i = 0; i < evaluators.length; i++) {
            ValueVector v = padConstant(evaluators[i].eval());
            MaterializedField f = MaterializedField.create(new SchemaPath(paths[i], ExpressionPosition.UNKNOWN), v.getField().getType());
            v.setField(f);
            outputVectors.add(v);
            if (new_schema) {
              schemaBuilder.addField(v.getField());
            }
          }
          for(ValueVector v : incoming){
            v.close();
          }
          if (new_schema) {
            batchSchema = schemaBuilder.build();
            new_schema = false;
          }
        } catch (Exception e) {
          logger.error(e.getMessage());
          e.printStackTrace();
          context.fail(e);
          return IterOutcome.STOP;
        }
    }
    return o;
  }

  private ValueVector padConstant(ValueVector v) {
    if (v.getAccessor().getValueCount() < recordCount) {
      Object constant = v.getAccessor().getObject(0);
      v.clear();
      AllocationHelper.allocate(v, recordCount, 8);
      ValueVector.Mutator mutator = v.getMutator();
      for (int i = 0; i < recordCount; i++) {
        mutator.setObject(i, constant);
      }
      mutator.setValueCount(recordCount);
    }
    return v;
  }

  @Override
  public void releaseAssets() {
      for(ValueVector v : outputVectors){
        v.close();
      }
  }
}
