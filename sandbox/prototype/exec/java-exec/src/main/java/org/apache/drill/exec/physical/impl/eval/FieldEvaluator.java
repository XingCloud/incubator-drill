package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:16 PM
 */
public class FieldEvaluator implements BasicEvaluator {

    private SchemaPath path;
    private RecordBatch recordBatch;

    public FieldEvaluator(SchemaPath path, RecordBatch recordBatch) {
        this.path = path;
        this.recordBatch = recordBatch;
    }

    @Override
    public ValueVector eval() {
        for (VectorWrapper<?> v : recordBatch) {
            if (v.getField().matches(path)) {
              TransferPair tp  =  v.getValueVector().getTransferPair() ;
              tp.mirror();
              return  tp.getTo();
            }
        }
        throw new DrillRuntimeException("Field not found : " + path);
    }
}
