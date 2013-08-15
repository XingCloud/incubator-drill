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
    submitQuery("/plans/common_noseg.json", QueryType.LOGICAL);
  }

  @Test
  public void test2() throws  Exception{
    submitQuery("/plans/common_seg.json", QueryType.LOGICAL);
  }

  @Test
  public void test3() throws  Exception{
    submitQuery("/plans/min5_noseg.json", QueryType.LOGICAL);
  }

  @Test
  public void test4() throws  Exception{
    submitQuery("/plans/min5_segment.json", QueryType.LOGICAL);
  }

  @Test
  public void test5() throws  Exception{
    submitQuery("/plans/event_noseg.json", QueryType.LOGICAL);
  }

  @Test
  public void test6() throws  Exception{
    submitQuery("/plans/logical_test6.json", QueryType.LOGICAL);
  }



}
