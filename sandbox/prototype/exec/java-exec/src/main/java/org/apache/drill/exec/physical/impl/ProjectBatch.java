package org.apache.drill.exec.physical.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.vector.Bit;
import org.apache.drill.exec.record.vector.ValueVector;

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
    private PathSegment paths[];


    private BatchSchema batchSchema;
    private boolean isFirst = true;
    private SchemaBuilder schemaBuilder;


    public ProjectBatch(FragmentContext context, Project config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
        this.selections = config.getExprs();
        this.paths = new PathSegment[selections.size()];
        this.evaluators = new BasicEvaluator[selections.size()];
        setupEvals();
    }

    @Override
    public void setupEvals() {
        EvaluatorFactory builder = new BasicEvaluatorFactory();
        for (int i = 0; i < selections.size(); i++) {
            paths[i] = selections.get(i).getRef().getRootSegment().getChild();
            evaluators[i] = builder.getBasicEvaluator(incoming.getRecordPointer(), selections.get(i).getExpr());
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
        fields.clear();

        IterOutcome o = incoming.next();
        switch (o) {
            case NONE:
            case STOP:
                fields.clear();
                recordCount = 0;
                break;

            case OK_NEW_SCHEMA:
                isFirst = true;
                schemaBuilder = BatchSchema.newBuilder();
            case OK:
                MaterializedField f;
                for (int i = 0; i < evaluators.length; i++) {
                    DrillValue dv = evaluators[i].eval();
                    ValueVector vv = (ValueVector) dv;
                    recordCount = vv.getRecordCount();
                    f = MaterializedField.create(new SchemaPath(selections.get(i).getRef().getPath()), i, 0, vv.getField().getType());
                    vv.setField(f);
                    fields.put(f.getFieldId(), vv);
                    if (isFirst) {
                        schemaBuilder.addField(f);
                    }
                }
                if (isFirst) {
                    try {
                        batchSchema = schemaBuilder.build();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                isFirst = false;
                record.set(batchSchema.getFields(), fields);
        }
        return o;
    }
}
