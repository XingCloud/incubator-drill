package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.config.MysqlScanPOP;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTER;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;

public class MysqlStorageEngine extends AbstractStorageEngine {

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

  public AbstractGroupScan getPhysicalScan(Scan scan, QueryContext context) throws IOException {
    JSONOptions root = scan.getSelection();
    if (root == null) {
      throw new NullPointerException("Selection is null");
    }
    JsonNode selection = root.getRoot(), projections;
    String tableName, filter = null;
    List<MysqlScanPOP.MysqlReadEntry> readEntries = Lists.newArrayList();
    List<NamedExpression> projectionList = Lists.newArrayList();
    tableName = selection.get(SELECTION_KEY_WORD_TABLE).textValue();
    if (selection.get(SELECTION_KEY_WORD_FILTER) != null)
      filter = selection.get(SELECTION_KEY_WORD_FILTER).textValue();
    projections = selection.get(SELECTION_KEY_WORD_PROJECTIONS);
    ObjectMapper mapper = context.getConfig().getMapper();
    for (JsonNode projection : projections) {
      try {
        projectionList.add(mapper.readValue(projection.toString(), NamedExpression.class));
      } catch (IOException e) {
        throw new NullPointerException("Cannot parse projection : " + projection.toString());
      }
    }
    readEntries.add(new MysqlScanPOP.MysqlReadEntry(tableName, filter, projectionList));
    return new MysqlScanPOP(readEntries);
  }

}
