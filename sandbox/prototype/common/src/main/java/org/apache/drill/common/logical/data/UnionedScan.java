package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

@JsonTypeName("unioned-scan")
public class UnionedScan extends SourceOperator {
  private final String storageEngine;
  private final JSONOptions selection;
  private final FieldReference outputReference;

  @JsonCreator
  public UnionedScan(@JsonProperty("storageengine") String storageEngine, @JsonProperty("selection") JSONOptions selection, @JsonProperty("ref") FieldReference outputReference) {
    super();
    this.storageEngine = storageEngine;
    this.selection = selection;
    this.outputReference = outputReference;
  }

  @JsonProperty("storageengine")
  public String getStorageEngine() {
    return storageEngine;
  }

  @JsonProperty("selection")
  public JSONOptions getSelection() {
    return selection;
  }

  @JsonProperty("ref")
  public FieldReference getOutputReference() {
    return outputReference;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitUnionedScan(this, value);
  }
}
