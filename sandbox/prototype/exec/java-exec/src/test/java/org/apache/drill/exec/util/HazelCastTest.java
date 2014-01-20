package org.apache.drill.exec.util;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Test;

import java.util.Map;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/14/14
 * Time: 6:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class HazelCastTest {
  @Test
  public void test1(){
    Config cfg = new Config();
    HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
    testMapAndQueue(instance);
    HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
    testMapAndQueue(instance1);
    System.out.println("hhh");
    getStartedClient();
  }

  private void testMapAndQueue(HazelcastInstance instance){
    Map<Integer, String> mapCustomers = instance.getMap("customers");
    mapCustomers.put(1, "Joe");
    mapCustomers.put(2, "Ali");
    mapCustomers.put(3, "Avi");

    System.out.println("Customer with key 1: "+ mapCustomers.get(1));
    System.out.println("Map Size:" + mapCustomers.size());

    Queue<String> queueCustomers = instance.getQueue("customers");
    queueCustomers.offer("Tom");
    queueCustomers.offer("Mary");
    queueCustomers.offer("Jane");
    System.out.println("First customer: " + queueCustomers.poll());
    System.out.println("Second customer: "+ queueCustomers.peek());
    System.out.println("Queue size: " + queueCustomers.size());
  }

  private void  getStartedClient(){
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.addAddress("127.0.0.1:5701");
    HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
    IMap map = client.getMap("customers");
    System.out.println("Map Size:" + map.size());
  }
}
