package org.apache.drill.exec.physical.impl.unionedscan;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.HBaseRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiEntryHBaseRecordReader implements RecordReader {
  private HbaseScanPOP.HbaseScanEntry[] entries;

  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private List<List<HbaseScanPOP.FilterEntry>> entryFilters;


  private int entryIndex;
  private ValueVector entryIndexVector;
  private List<NamedExpression> projections;
  private List<NamedExpression[]> entryProjections;
  private List<HBaseFieldInfo[]> entryProjFieldInfos;
  private List<ValueVector[]> entryValueVectors;
  private OutputMutator outputMutator;
  private boolean parseRk=false;
  
  public MultiEntryHBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry[] config) {
    this.context=context;
    this.entries=config;
  }

  private void initConfig() throws Exception {
    this.startRowKey= HBaseRecordReader.parseRkStr(entries[0].getStartRowKey());
    this.endRowKey= HBaseRecordReader.parseRkStr(entries[entries.length-1].getEndRowKey());
    this.tableName=entries[0].getTableName();
    this.entryFilters=new ArrayList<>();
    this.entryIndex=0;
    this.projections=new ArrayList<>();
    this.entryProjections=new ArrayList<>();

    List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
    Map<String,HBaseFieldInfo> fieldInfoMap= new HashMap<>();
    for (HBaseFieldInfo col : cols) {
        fieldInfoMap.put(col.fieldSchema.getName(), col);
    }
    for(int i=0;i<entries.length;i++){
       this.entryFilters.add(entries[i].getFilters());
       List<NamedExpression> exprs=entries[i].getProjections();
       NamedExpression[] exprArr=new NamedExpression[exprs.size()];
       HBaseFieldInfo[] infos=new HBaseFieldInfo[exprs.size()];
       for(int j=0;j<exprs.size();j++){
           exprArr[j]=exprs.get(j);
           infos[j]=fieldInfoMap.get(((SchemaPath)exprArr[j].getExpr()).getPath().toString());
           if(!projections.contains(exprArr[j]))
               projections.add(exprArr[j]);
           if (false == parseRk && infos[j].fieldType == HBaseFieldInfo.FieldType.rowkey)
               parseRk = true;
       }
       entryProjections.add(exprArr);
       entryProjFieldInfos.add(infos);
    }
  }

  private void initTableScanner() {

  }

    
  @Override
  public void setup(OutputMutator output) throws Exception {
      this.outputMutator=output;
      initConfig();
      initTableScanner();
      for (int i = 0; i < entryProjFieldInfos.size(); i++) {
          HBaseFieldInfo[] infos=entryProjFieldInfos.get(i);
          ValueVector[] valueVectors=new ValueVector[infos.length];
          for(int j=0;j<infos.length;j++){
              TypeProtos.MajorType type = HBaseRecordReader.getMajorType(infos[j]);
              //int batchRecordCount = batchSize;
              valueVectors[i] =
                      getVector(infos[j].fieldSchema.getName(),type);
              output.addField(valueVectors[i]);
              output.setNewSchema();
          }
          entryValueVectors.add(valueVectors);
      }
  }



    private ValueVector getVector( String name, TypeProtos.MajorType type) {
      if (type.getMode() != TypeProtos.DataMode.REQUIRED) throw new UnsupportedOperationException();
      MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
      if(context==null)return TypeHelper.getNewVector(f, new DirectBufferAllocator());
      return TypeHelper.getNewVector(f, context.getAllocator());
  }

  @Override
  public int next() {
    return 0;  //TODO method implementation
  }

  @Override
  public void cleanup() {
    //TODO method implementation
  }
}
