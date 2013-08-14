package org.apache.drill.exec.client;

import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos.*;
import org.junit.Test;

import static org.apache.drill.exec.client.TestQueryPhysical.submitQuery ;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/14/13
 * Time: 5:35 PM
 */
public class TestQueryLogical extends PopUnitTestBase {

  @Test
  public void test1() throws  Exception{
    submitQuery("/plans/logical_test1.json", QueryType.LOGICAL);
  }
}
