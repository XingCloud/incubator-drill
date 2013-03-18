package org.apache.drill.adhoc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.data.*;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:36
 * To change this template use File | Settings | File Templates.
 */
public class AdhocSQLQueryVisitorImpl implements SelectVisitor {

    private List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>();
    private NamedExpression[] selections = null;

    public List<LogicalOperator> getLogicalOperators() {
        return logicalOperators;
    }

    public List<String> getSelections(){
        List<String> _selections = new ArrayList<>();
        for(NamedExpression namedExpression:selections){
            _selections.add(namedExpression.getRef().getPath().toString());
        }
        return _selections;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        //get the where expression first for hbase/mysql scan
        Expression where = plainSelect.getWhere();
        LogicalExpression whereExpr = null;
        if (where != null) {
            AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
            where.accept(exprVisitor);
            whereExpr = exprVisitor.getLogicalExpression();
        }

        //scan
        FromItem item = plainSelect.getFromItem();
        AdhocFromItemVisitorImpl fromVisitor = new AdhocFromItemVisitorImpl(whereExpr);
        item.accept(fromVisitor);
        LogicalOperator fromLop = fromVisitor.getLogicalOperator();
        logicalOperators.add(fromLop);
        if (fromLop instanceof Join) {
            logicalOperators.add(((Join) fromLop).getLeft());
            logicalOperators.add(((Join) fromLop).getRight());
        }

        //filter
        SingleInputOperator filter = null;
        if(whereExpr!=null){
            filter = new Filter(whereExpr);
            filter.setInput(fromLop);
            logicalOperators.add(filter);
        }

        //Get select item expressions && distinct
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        Distinct distinct = null;
        List<LogicalExpression> selectItemlogicalExpressions = new ArrayList<LogicalExpression>();
        for(SelectItem selectItem : selectItems) {
            AdhocSelectItemVisitorImpl selectItemVisitor = new AdhocSelectItemVisitorImpl();
            selectItem.accept(selectItemVisitor);
            LogicalExpression logicalExpression = selectItemVisitor.getLogicalExpr();
            selectItemlogicalExpressions.add(logicalExpression);

            //distinct
            if (selectItemVisitor.isDistinct()) {
                if (logicalExpression instanceof FieldReference) {
                    distinct = new Distinct((FieldReference) logicalExpression);
                } else if (logicalExpression instanceof FunctionCall) {
                    FieldReference ref = (FieldReference)((FunctionCall) logicalExpression).args.get(0);
                    distinct = new Distinct(ref);
                }

                if (filter != null) {
                    distinct.setInput(filter);
                } else {
                    distinct.setInput(fromLop);
                }

                logicalOperators.add(distinct);
            }
        }

        //segment
        Segment segment = null;
        List<LogicalExpression> groupbyLogicalExpressions = new ArrayList<LogicalExpression>();
        List<Expression> groupbyExpressions = plainSelect.getGroupByColumnReferences();
        if (groupbyExpressions != null) {
            for (Expression groupbyExpression : groupbyExpressions) {
                AdhocExpressionVisitorImpl ev = new AdhocExpressionVisitorImpl();
                groupbyExpression.accept(ev);
                groupbyLogicalExpressions.add(ev.getLogicalExpression());
            }
        }
        if (groupbyLogicalExpressions.size() != 0) {
             segment = new Segment((LogicalExpression[])groupbyLogicalExpressions.toArray(), new FieldReference("segment"));
            if (distinct != null) {
                segment.setInput(distinct);
            } else if (filter != null){
                segment.setInput(filter);
            } else {
                segment.setInput(fromLop);
            }
            logicalOperators.add(segment);
        }

        //collapsing aggregate
        selections = changeToNamedExpressions(selectItemlogicalExpressions);
        CollapsingAggregate collapsingAggregate = getCollapsingAggregate(selections, segment);
        if (collapsingAggregate!=null){
            if(segment !=null){
                collapsingAggregate.setInput(segment);
            }else if (distinct !=null){
                collapsingAggregate.setInput(distinct);
            }else if (filter !=null){
                collapsingAggregate.setInput(filter);
            }else{
                collapsingAggregate.setInput(fromLop);
            }
            logicalOperators.add(collapsingAggregate);
        }
//
//
//        //project
        Project project =  null;
        if (collapsingAggregate == null) {
            project = new Project(changeToFieldRefOnly(selections)); //todo add output prefix
            if(segment!=null){
                project.setInput(segment);
            }else if (distinct !=null){
                collapsingAggregate.setInput(distinct);
            }else if (filter!=null){
                project.setInput(filter);
            }else{
                project.setInput(fromLop);
            }
            logicalOperators.add(project);
        }else{
            //do nothing
        }

        //Get output logical operator
        Store store = getStore();
        if(project != null){
            store.setInput(project);
        }else {
            store.setInput(collapsingAggregate);
        }
        logicalOperators.add(store);
    }

    @Override
    public void visit(Union union) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private NamedExpression[] changeToNamedExpressions(List<LogicalExpression> logicalExpressions) {
        List<NamedExpression> namedExpressions = new ArrayList<NamedExpression>();
        for (LogicalExpression exprTmp : logicalExpressions) {
            if (exprTmp instanceof FieldReference) {
                NamedExpression namedExpression = new NamedExpression(exprTmp, (FieldReference) exprTmp);
                namedExpressions.add(namedExpression);
            } else if (exprTmp instanceof FunctionCall){
                LogicalExpression ref = ((FunctionCall) exprTmp).args.get(0);
                String functionName = ((FunctionCall)exprTmp).getDefinition().getName();
                FieldReference newFieldRef = new FieldReference(functionName+"."+((SchemaPath)ref).getPath());
                NamedExpression namedExpression = new NamedExpression(exprTmp, newFieldRef);
                namedExpressions.add(namedExpression);
            }
        }

        return namedExpressions.toArray(new NamedExpression[0]);
    }

    private NamedExpression[] changeToFieldRefOnly(NamedExpression[] namedExpressions) {
        List<NamedExpression> list = new ArrayList<NamedExpression>();
        for (NamedExpression namedExpression : namedExpressions) {
            LogicalExpression expr = namedExpression.getExpr();
            if (expr instanceof FunctionCall) {
                NamedExpression nameExpr = new NamedExpression(namedExpression.getRef(), new FieldReference("output"));//wcl
                list.add(nameExpr);
            } else {
                list.add(new NamedExpression(namedExpression.getRef(), new FieldReference("output")));
            }
        }
        return list.toArray(new NamedExpression[0]);
    }

    private CollapsingAggregate getCollapsingAggregate(NamedExpression[] namedExpressions, Segment segment) {
        FieldReference within = null;
        if (segment != null){
            within = new FieldReference("segment");
        }
        FieldReference target = null;
        List<FieldReference> carryovers = new ArrayList<>();
        List<NamedExpression> _namedExpressions = new ArrayList<>();

        for (NamedExpression namedExpression : namedExpressions) {
            LogicalExpression expr = namedExpression.getExpr();
            if (expr instanceof FunctionCall) {
                if (((FunctionCall) expr).getDefinition().getName().equals("count") ||
                        ((FunctionCall) expr).getDefinition().getName().equals("sum")) {
                    _namedExpressions.add(namedExpression);
                }
            } else{
                carryovers.add(namedExpression.getRef());
            }
        }
        if (_namedExpressions.size()>0){
            return new CollapsingAggregate(within,target,carryovers.toArray(new FieldReference[carryovers.size()]),_namedExpressions.toArray(new NamedExpression[_namedExpressions.size()]));
        }

        return  null;
    }


    private Store getStore(){
        try{
            ObjectMapper mapper = new ObjectMapper();
            //return new Store("console", mapper.readValue(new String("{\"pipe\":\"STD_OUT\"}").getBytes(),JSONOptions.class), null);
            return new Store("fs", mapper.readValue(new String("{\"file\":\"/home/hadoop/scan_result\", \"type\":\"JSON\"}").getBytes(),JSONOptions.class), null);
        }catch (Exception e){
            //todo wcl
            return null;
        }
    }

}
