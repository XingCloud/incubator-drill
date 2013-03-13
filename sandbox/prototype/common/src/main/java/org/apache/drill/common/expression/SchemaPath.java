/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.common.expression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.common.expression.visitors.ExprVisitor;

public class SchemaPath extends LogicalExpressionBase{

  // reads well in RegexBuddy
  private static final String ENTIRE_REGEX = "^\n" +
      "(?:                # first match required\n" +
      "\\[\\d+\\]             # array index only\n" +
      "|\n" +
      "'?\n" +
      "[^\\.\\[\\+\\-\\!\\]\\}]+  # identifier\n" +
      "'?\n" +
      "(?:\\[\\d+\\])?\n" +
      ")\n" +
      "[\\+\\-\\!\\]\\}]?\n" +

      "# secondary matches (starts with dot)\n" +
      "(?:\n" +
      "\\.\n" +
      "(?:                # first match required\n" +
      "\\[\\d+\\]             # array index only\n" +
      "|\n" +
      "'?\n" +
      "[^\\.\\[\\+\\-\\!\\]\\}]+  # identifier\n" +
      "'?\n" +
      "(?:\\[\\d+\\])?\n" +
      ")\n" +
      "[\\+\\-\\!\\]\\}]?\n" +

      ")*$";
  
  // reads well in RegexBuddy
  private static final String SEGMENT_REGEX = "(?:\n" +
      "(\\[\\d+\\])\n" +
      "|\n" +
      "'?\n" +
      "([^\\.\\[\\+\\-\\!\\]\\}]+)  # identifier\n" +
      "'?\n" +
      ")\n" +
      "([\\+\\-\\!\\]\\}]?)         # collision type";
  private static final int GROUP_INDEX = 1;
  private static final int GROUP_PATH_SEGMENT = 2;
  private static final int GROUP_COLLISION = 3;
  
  
  private final static Pattern SEGMENT_PATTERN = Pattern.compile(SEGMENT_REGEX, Pattern.COMMENTS);
  private final static Pattern ENTIRE_PATTERN = Pattern.compile(ENTIRE_REGEX, Pattern.COMMENTS);
  
  private final CharSequence originalPath;
  private final PathSegment rootSegment;
  
  
  
	public SchemaPath(CharSequence str) {
	  //if(!ENTIRE_PATTERN.matcher(str).matches()) throw new IllegalArgumentException("Identifier doesn't match expected pattern.");//wcl
	  this.originalPath = str;
	  Matcher m = SEGMENT_PATTERN.matcher(str);
		PathSegment r = null;
		PathSegment previous = null;
		PathSegment current;
		while(m.find()){
		  CollisionBehavior col =  (m.start(GROUP_COLLISION) != -1) ? CollisionBehavior.find(m.group(GROUP_COLLISION)) : CollisionBehavior.DEFAULT;

      if(m.start(GROUP_INDEX) != -1){
        String d = m.group(GROUP_INDEX);
        current = new PathSegment.ArraySegment(Integer.parseInt(d), col);
      }else{
        String i = m.group(GROUP_PATH_SEGMENT);
        current = new PathSegment.NameSegment(i, col);
      }
		  if(previous == null){
		    r = current;
		  }else{
		    previous.setChild(current);
		  }
		  previous = current;
		}
		
		rootSegment = r;
		

	}
		
	
	
	@Override
  public <T> T accept(ExprVisitor<T> visitor) {
    return visitor.visitSchemaPath(this);
  }


  public PathSegment getRootSegment(){
	  return rootSegment;
	}
	
	public CharSequence getPath(){
	  return originalPath;
	}
	
  @Override
  public void addToString(StringBuilder sb) {
    sb.append("'");
    sb.append(originalPath);
    sb.append("'");
  }



  @Override
  public String toString() {
    return "SchemaPath [rootSegment=" + rootSegment + "]";
  }

    public static void main(String[] args) throws Exception{
        SchemaPath schemaPath = new SchemaPath("sof-dsk_deu");
        Matcher m = SEGMENT_PATTERN.matcher("sof-dsk_deu");
        m.find();
        int i = m.start(GROUP_COLLISION);
        String string = m.group(GROUP_COLLISION);
        String xx = m.group(GROUP_PATH_SEGMENT);

        i = m.start(GROUP_INDEX);
        xx = m.group(GROUP_INDEX);

        xx = m.group(GROUP_PATH_SEGMENT);

        System.out.println("xx");
    }
  

}