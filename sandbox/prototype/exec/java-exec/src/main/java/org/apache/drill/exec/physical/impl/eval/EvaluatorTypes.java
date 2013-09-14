package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:45 PM
 */
public interface EvaluatorTypes {

  public interface BasicEvaluator {
    public ValueVector eval();
  }

  public interface BooleanEvaluator {
    public BitVector eval();
  }

  public interface ConstantEvaluator {
    public ValueVector eval();
  }

  public interface AggregatingEvaluator extends BasicEvaluator {
    public void addBatch();
    public void setWithin(BasicEvaluator boundary);
  }

  public abstract class  AbstractAggregatingEvaluator implements AggregatingEvaluator{
    protected BasicEvaluator boundary ;
    protected BasicEvaluator child;
    protected RecordBatch recordBatch;
    protected BigIntVector value;

    public void setWithin(BasicEvaluator boundary) {
      this.boundary = boundary;
    }

    public int getBoundary(){
      int boundaryId = 0 ;
      IntVector intVector = (IntVector) boundary.eval() ;
      boundaryId = intVector.getAccessor().get(0);
      intVector.close();
      return  boundaryId ;
    }

  }

}
