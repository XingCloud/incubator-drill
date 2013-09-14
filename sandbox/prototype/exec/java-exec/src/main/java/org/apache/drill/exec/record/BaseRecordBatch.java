package org.apache.drill.exec.record;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/8/13
 * Time: 10:18 PM
 */
public abstract class BaseRecordBatch implements RecordBatch {

    protected List<ValueVector> outputVectors = Lists.newArrayList();
    protected VectorHolder vh ;
    protected int recordCount = 0 ;

    public abstract void setupEvals();


    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
        return vh.getValueVector(path);
    }

    @Override
    public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
           return vh.getValueVector(fieldId,clazz);
    }

    public SelectionVector2 getSelectionVector2() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableBatch getWritableBatch() {
        return WritableBatch.get(this);
    }

    @Override
    public Iterator<ValueVector> iterator() {
        return outputVectors.iterator();
    }

    @Override
    public int getRecordCount() {
        return recordCount;
    }

    public  abstract  void releaseAssets();
}
