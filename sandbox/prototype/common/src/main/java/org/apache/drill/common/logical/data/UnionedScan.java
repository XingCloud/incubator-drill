package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

@JsonTypeName("unioned-scan")
public class UnionedScan extends SourceOperator{
  private final String storageEngine;
 	private final JSONOptions[] selections;
 	private final FieldReference[] outputReferences;
 	
 	@JsonCreator
   public UnionedScan(@JsonProperty("storageengine") String storageEngine, @JsonProperty("selections") JSONOptions[] selections, @JsonProperty("refs") FieldReference[] outputReferences) {
     super();
     this.storageEngine = storageEngine;
     this.selections = selections;
     this.outputReferences = outputReferences;
   }
 
   @JsonProperty("storageengine")
   public String getStorageEngine() {
     return storageEngine;
   }
 
   public JSONOptions[] getSelections() {
     return selections;
   }
 
   @JsonProperty("ref")
   public FieldReference[] getOutputReferences() {
     return outputReferences;
   }
 
   @Override
   public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
       return logicalVisitor.visitUnionedScan(this, value);
   }
}
