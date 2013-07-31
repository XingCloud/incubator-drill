package org.apache.drill.outer.selections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.expression.ExpressionPosition;

/**
 * User: Z J Wu
 * Date: 13-7-30
 * Time: 上午10:01
 * Package: org.apache.drill.outer.selections
 */
public class GenericUtils {
  public static final ObjectMapper GENERIC_OBJECT_MAPPER = new ObjectMapper();
  public static final ExpressionPosition NULL_EXPR_POSI = null;
}
