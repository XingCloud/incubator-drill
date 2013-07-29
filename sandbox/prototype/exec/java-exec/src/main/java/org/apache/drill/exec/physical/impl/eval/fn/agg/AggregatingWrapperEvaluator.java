package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/10/13
 * Time: 5:27 PM
 */
public class AggregatingWrapperEvaluator implements AggregatingEvaluator {

    private List<AggregatingEvaluator> args;
    private BasicEvaluator topEvaluator;

    public AggregatingWrapperEvaluator(BasicEvaluator topEvaluator, List<AggregatingEvaluator> args) {
        super();
        this.topEvaluator = topEvaluator;
        this.args = args;
    }

    @Override
    public void addBatch() {
        for (AggregatingEvaluator aggregatingEvaluator : args) {
            aggregatingEvaluator.addBatch();
        }
    }

    @Override
    public ValueVector eval() {
        return topEvaluator.eval();
    }

    public CountDistinctAggregator getCountDistinctAggregator(){
      if(topEvaluator instanceof  CountDistinctAggregator){
        return  (CountDistinctAggregator) topEvaluator ;
      }

      for(BasicEvaluator evaluator : args){
        if(evaluator instanceof  CountDistinctAggregator){
          return (CountDistinctAggregator) evaluator ;
        }
      }

      return  null ;
    }

}
