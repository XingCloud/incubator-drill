package org.apache.drill.exec.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.UnionedScan;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.UnionedScanPOP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.drill.common.util.Selections.*;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/11/13
 * Time: 12:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseStorageEngine extends AbstractStorageEngine {

  private static HBaseStorageEngine instance = new HBaseStorageEngine();

  public static HBaseStorageEngine getInstance() {
    return instance;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public List<QueryOptimizerRule> getOptimizerRules() {
    return Collections.emptyList();
  }

  @Override
  public AbstractGroupScan getPhysicalScan(Scan scan, QueryContext context) throws IOException {
    JSONOptions selections = scan.getSelection();
    if (selections == null) {
      throw new NullPointerException("Selection is null");
    }
    try {
      if (scan instanceof UnionedScan) {
        JSONOptions selection = scan.getSelection();
        FieldReference ref = scan.getOutputReference();
        if (selection == null) {
          throw new NullPointerException("Selection is null");
        }
        List<HbaseScanPOP.HbaseScanEntry> entries = new ArrayList<>();
        createHbaseScanEntry(selection, ref, entries, context);
        UnionedScanPOP pop = new UnionedScanPOP(entries);
        return pop;
      } else {
        List<HbaseScanPOP.HbaseScanEntry> entries = new ArrayList<>(selections.getRoot().size());
        createHbaseScanEntry(selections, scan.getOutputReference(), entries, context);
        HbaseScanPOP pop = new HbaseScanPOP(entries);
        return pop;
      }
    } catch (OptimizerException e) {
      throw new IOException("optimize failed!", e);
    }
  }

  private void createHbaseScanEntry(JSONOptions selections, FieldReference ref, List<HbaseScanPOP.HbaseScanEntry> entries, QueryContext context) throws
    OptimizerException {
    //TODO where is ref?
    ObjectMapper mapper = context.getConfig().getMapper();
    JsonNode root = selections.getRoot(), filters, projections, rowkey;
    String table, rowkeyStart, rowkeyEnd, projectionString, filterString, filterType;
    int selectionSize = root.size();
    HbaseScanPOP.HbaseScanEntry entry;
    List<LogicalExpression> filterList;
    LogicalExpression le;
    List<NamedExpression> projectionList;
    NamedExpression ne;
    List<HbaseScanPOP.RowkeyFilterEntry> filterEntries;
    HbaseScanPOP.RowkeyFilterEntry rowkeyFilterEntry;
    FieldReference filterTypeFR;
    for (JsonNode selection : root) {
      // Table name
      table = selection.get(SELECTION_KEY_WORD_TABLE).textValue();

      // Rowkey range
      rowkey = selection.get(SELECTION_KEY_WORD_ROWKEY);
      rowkeyStart = rowkey.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
      rowkeyEnd = rowkey.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();

      // Filters
      filters = selection.get(SELECTION_KEY_WORD_FILTERS);
      if (filters != null) {
        filterEntries = new ArrayList<>(filters.size());
        for (JsonNode filterNode : filters) {
          filterType = filterNode.get(SELECTION_KEY_WORD_FILTER_TYPE).textValue();
          if ("ROWKEY".equals(filterType)) {
            JsonNode includes = filterNode.get(SELECTION_KEY_WORD_ROWKEY_INCLUDES);
            if (includes == null || includes.size() == 0) {
              throw new OptimizerException("Rowkey filter must have at least one include.");
            }
            filterList = new ArrayList<>(includes.size());
            for (JsonNode include : includes) {
              try {
                le = mapper.readValue(include.traverse(), LogicalExpression.class);
              } catch (Exception e) {
                throw new OptimizerException("Cannot parse filter - " + include.textValue());
              }
              filterList.add(le);
            }
            filterTypeFR = new FieldReference("XARowKeyPatternFilter", ExpressionPosition.UNKNOWN);
            rowkeyFilterEntry = new HbaseScanPOP.RowkeyFilterEntry(filterTypeFR, filterList);
            filterEntries.add(rowkeyFilterEntry);
          }
        }
      } else {
        filterEntries = null;
      }

      // Projections
      projections = selection.get(SELECTION_KEY_WORD_PROJECTIONS);
      projectionList = new ArrayList<>(projections.size());
      for (JsonNode projectionNode : projections) {
        projectionString = projectionNode.toString();
        try {
          ne = mapper.readValue(projectionString, NamedExpression.class);
        } catch (IOException e) {
          throw new OptimizerException("Cannot parse projection - " + projectionString);
        }
        projectionList.add(ne);
      }
      entry = new HbaseScanPOP.HbaseScanEntry(table, rowkeyStart, rowkeyEnd, filterEntries, projectionList);
      entries.add(entry);
    }
  }
  /*
    @Override
    public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
        if(readEntry instanceof HbaseScanPOP.HbaseScanEntry)
            return new HBaseRecordReader(context,(HbaseScanPOP.HbaseScanEntry)readEntry);
        return null;
    }
*/
}
