package org.apache.drill.exec.physical.impl.eval.fn;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/5/13
 * Time: 7:08 PM
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface FunctionEvaluator {
    String value();
}
