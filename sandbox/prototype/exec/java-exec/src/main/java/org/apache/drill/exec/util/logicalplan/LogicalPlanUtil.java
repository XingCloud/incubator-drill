package org.apache.drill.exec.util.logicalplan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.exception.OptimizerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTER;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/25/13
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogicalPlanUtil {
    public static Logger logger = LoggerFactory.getLogger(LogicalPlanUtil.class);

    public static String getRkPattern(Scan scan, DrillConfig config) throws IOException {
        JsonNode filterNode = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        String tableName = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_TABLE).textValue();
        try {
            List<KeyPart> kps = TableInfo.getRowKey(tableName, null);
            String rkPattern = "";
            Map<String, UnitFunc> fieldFunc = parseFilterExpr(filterNode, config);
            List<KeyPart> workKps = kps;
            Deque<KeyPart> toWorkKps = new ArrayDeque<>(workKps);
            loop:
            while (workKps.size() > 0) {
                for (KeyPart kp : workKps) {
                    if (kp.getType() == KeyPart.Type.field) {
                        String value;
                        UnitFunc unitFunc = fieldFunc.get(kp.getField().getName());
                        if (unitFunc != null)
                            value = unitFunc.getValue();
                        else
                            value = "*";
                        rkPattern += value;
                        toWorkKps.removeFirst();
                        fieldFunc.remove(kp.getField().getName());
                        if (fieldFunc.size() == 0)
                            break loop;
                    } else if (kp.getType() == KeyPart.Type.constant) {
                        rkPattern += kp.getConstant();
                        toWorkKps.removeFirst();
                    } else {
                        toWorkKps.removeFirst();
                        for (int i = kp.getOptionalGroup().size() - 1; i >= 0; i--) {
                            toWorkKps.addFirst(kp.getOptionalGroup().get(i));
                        }
                        break;
                    }
                }
                workKps = Arrays.asList(toWorkKps.toArray(new KeyPart[toWorkKps.size()]));
            }
            if (rkPattern.endsWith("."))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 1);
            while (rkPattern.endsWith(".*"))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 2);

            return rkPattern;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
    }

    public static boolean needIncludes(JsonNode filterNode,DrillConfig config,String tableName) throws OptimizerException {
        LogicalExpression le= null;
      try {
        logger.debug("test needIncludes for "+config.getMapper().writeValueAsString(filterNode));
      } catch (JsonProcessingException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      try {
            le = config.getMapper().readValue(filterNode.get("expression").traverse(),LogicalExpression.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new OptimizerException(e.getMessage());
        }
        if(!(le instanceof FunctionCall))
            return false;
        List<String> rkPatterns=getRkPatterns((FunctionCall)le,config,tableName);
        for(String rkPattern : rkPatterns){
            if(!rkPattern.contains("*."))
                return false;
        }
        return true;
    }
    public static List<String> getRkPatterns(FunctionCall filterExpr,DrillConfig config,String tableName) throws OptimizerException {
        List<String> rkPatterns=new ArrayList<>();
        if(!filterExpr.getDefinition().getName().contains("or"))
            rkPatterns.add(getRkPattern(filterExpr,config,tableName));
        else
            for(LogicalExpression le : filterExpr){
                if(!(le instanceof FunctionCall))continue;
                rkPatterns.addAll(getRkPatterns(((FunctionCall) le), config, tableName));
            }
        return rkPatterns;
    }

    public static String getRkPattern(FunctionCall filterFunc,DrillConfig config,String tableName) throws  OptimizerException {
        try {
            List<KeyPart> kps = TableInfo.getRowKey(tableName, null);
            String rkPattern = "";
            Map<String, UnitFunc> fieldFunc = parseFunctionCall(filterFunc, config);
            List<KeyPart> workKps = kps;
            Deque<KeyPart> toWorkKps = new ArrayDeque<>(workKps);
            loop:
            while (workKps.size() > 0) {
                for (KeyPart kp : workKps) {
                    if (kp.getType() == KeyPart.Type.field) {
                        String value;
                        UnitFunc unitFunc = fieldFunc.get(kp.getField().getName());
                        if (unitFunc != null)
                            value = unitFunc.getValue();
                        else
                            value = "*";
                        rkPattern += value;
                        toWorkKps.removeFirst();
                        fieldFunc.remove(kp.getField().getName());
                        if (fieldFunc.size() == 0)
                            break loop;
                    } else if (kp.getType() == KeyPart.Type.constant) {
                        rkPattern += kp.getConstant();
                        toWorkKps.removeFirst();
                    } else {
                        toWorkKps.removeFirst();
                        for (int i = kp.getOptionalGroup().size() - 1; i >= 0; i--) {
                            toWorkKps.addFirst(kp.getOptionalGroup().get(i));
                        }
                        break;
                    }
                }
                workKps = Arrays.asList(toWorkKps.toArray(new KeyPart[toWorkKps.size()]));
            }
            if (rkPattern.endsWith("."))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 1);
            while (rkPattern.endsWith(".*"))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 2);

            return rkPattern;
        } catch (Exception e) {
            e.printStackTrace();
            throw new OptimizerException(e.getMessage());
        }

    }


    public static Map<String, UnitFunc> parseFilterExpr(JsonNode origExpr, DrillConfig config) throws IOException {
        LogicalExpression func = config.getMapper().readValue(origExpr.traverse(), LogicalExpression.class);
        return parseFunctionCall((FunctionCall) func, config);
    }

    public static Map<String, UnitFunc> parseFunctionCall(FunctionCall func, DrillConfig config) {
        Map<String, UnitFunc> result = new HashMap<>();
        String field = null;
        UnitFunc value = null;
        for (LogicalExpression le : func) {
            if (le instanceof FunctionCall) {
                for (Map.Entry<String, UnitFunc> entry : parseFunctionCall(((FunctionCall) le), config).entrySet()) {
                    if (result.containsKey(entry.getKey())) {
                        LogicalExpression old = result.get(entry.getKey()).getFunc();
                        FunctionRegistry registry = new FunctionRegistry(config);
                        FunctionCall call = (FunctionCall) registry.createExpression("&&", ExpressionPosition.UNKNOWN, Arrays.asList(old, entry.getValue().getFunc()));
                        UnitFunc resultFunc = new UnitFunc(call);
                        result.put(field, resultFunc);
                    } else
                        result.put(entry.getKey(), entry.getValue());
                }
            } else if (le instanceof SchemaPath) {
                field = ((SchemaPath) le).getPath().toString();
            } else if (le instanceof ValueExpressions.QuotedString) {
                value = new UnitFunc(func);
            }
        }
        if (field != null && value != null) {
            if (result.containsKey(field)) {
                LogicalExpression old = result.get(field).getFunc();
                FunctionRegistry registry = new FunctionRegistry(config);
                FunctionCall call = (FunctionCall) registry.createExpression("&&", ExpressionPosition.UNKNOWN, Arrays.asList(old, value.getFunc()));
                UnitFunc resultFunc = new UnitFunc(call);
                result.put(field, resultFunc);
            } else
                result.put(field, value);
        }
        return result;
    }

    public static List<UnitFunc> parseToUnit(FunctionCall call, DrillConfig config) {
        List<UnitFunc> result = new ArrayList<>();
        for (LogicalExpression le : call) {
            if (le instanceof FunctionCall) {
                result.addAll(parseToUnit((FunctionCall) le, config));
            } else {
                result.add(new UnitFunc(call));
            }
        }
        return result;
    }

    public static class UnitFunc {
        private String field;
        private String op;
        private String value;
        private FunctionCall func;

        public UnitFunc() {

        }

        public UnitFunc(FunctionCall func) {
            this.func = func;
            for (LogicalExpression le : func) {
                if (le instanceof SchemaPath) {
                    field = ((SchemaPath) le).getPath().toString();
                } else if (le instanceof ValueExpressions.QuotedString) {
                    value = ((ValueExpressions.QuotedString) le).value;
                }
            }
            op = func.getDefinition().getName();
        }

        public UnitFunc(String field, String op, String value) {
            this.field = field;
            this.op = op;
            this.value = value;
        }

        public String getField() {
            return field;
        }

        public String getOp() {
            return op;
        }

        public String getValue() {
            return value;
        }

        public FunctionCall getFunc() {
            return func;
        }

        public boolean equals(Object o) {
            if (!(o instanceof UnitFunc))
                return false;
            if (func.equals(((UnitFunc) o).getFunc()))
                return true;
            return false;
        }
    }
}

