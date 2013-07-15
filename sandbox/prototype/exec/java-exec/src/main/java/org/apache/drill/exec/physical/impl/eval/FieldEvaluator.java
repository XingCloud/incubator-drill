package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordPointer;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:16 PM
 */
public class FieldEvaluator implements BasicEvaluator {

    private SchemaPath path;
    private RecordPointer record;

    public FieldEvaluator(SchemaPath path, RecordPointer record) {
        this.path = path;
        this.record = record;
    }

    @Override
    public DrillValue eval() {
        for (MaterializedField f : record.getFieldsInfo()) {
            if (f.matches(path)) {
                return record.getFields().get(f.getFieldId());
            }
        }
        // TODO
        return null;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
