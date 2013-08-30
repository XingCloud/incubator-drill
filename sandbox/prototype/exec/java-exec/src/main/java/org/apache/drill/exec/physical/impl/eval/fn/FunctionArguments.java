package org.apache.drill.exec.physical.impl.eval.fn;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/5/13
 * Time: 6:37 PM
 */
public class FunctionArguments {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionArguments.class);

    private final boolean onlyConstants;
    private final boolean includesAggregates;
    private final List<BasicEvaluator> evals;
    private ObjectIntOpenHashMap<String> nameToPosition = new ObjectIntOpenHashMap<>();

    public FunctionArguments(boolean onlyConstants, boolean includesAggregates, List<BasicEvaluator> evals, FunctionCall call) {
        this.onlyConstants = onlyConstants;
        this.includesAggregates = includesAggregates;
        this.evals = evals;

        String names[] = call.getDefinition().getArgumentNames();
        for (int i = 0; i < names.length; i++) {
            if (names[i] != null)
                nameToPosition.put(names[i], i);
        }
    }

    public BasicEvaluator getEvaluator(String name) {
        if (!nameToPosition.containsKey(name)) throw new RuntimeException("Unknown Item provided.");
        return getEvaluator(nameToPosition.lget());
    }

    public BasicEvaluator getEvaluator(int index) {
        BasicEvaluator eval = evals.get(index);
        if (eval == null) throw new RuntimeException("Unknown Item provided.");
        return eval;
    }

    public BasicEvaluator getOnlyEvaluator() {
        // TODO
        if (evals.size() != 1) throw new RuntimeException("xx");
        return evals.get(0);

    }

    public int size() {
        return evals.size();
    }

    public BasicEvaluator[] getArgsAsArray() {
        return evals.toArray(new BasicEvaluator[size()]);
    }

    public boolean isOnlyConstants() {
        return onlyConstants;
    }
}
