package org.apache.helix.controller;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;


public class ZkClientMain {
  private static final String LOCAL_ZK = "localhost:2183";
  private static final String LARGE_NODE_PATH = "/test2MB";

  @Test
  public void testCreateLargeZnode() {
    System.setProperty("jute.maxbuffer", String.valueOf(1024 * 1024 * 5L));
    byte[] bytes = new byte[1024 * 1023];
    Arrays.fill(bytes, (byte) 1);
    String maxBufferString = System.getProperty("jute.maxbuffer");
    System.out.println(maxBufferString);
    ZkClient zkClient = new ZkClient(LOCAL_ZK);
    try {
      System.out.println("Attempt to create znode size: " + bytes.length);
      zkClient.createPersistent(LARGE_NODE_PATH, bytes);
    } catch (Exception e) {
      System.out.println("Error while creating the node" + e.getMessage());
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testReadLargeZnode() {
    System.setProperty("jute.maxbuffer", String.valueOf(1024 * 1024));
    String maxBufferString = System.getProperty("jute.maxbuffer");
    System.out.println(maxBufferString);
    ZkSerializer zkSerializer = new ZkSerializer() {
      @Override
      public byte[] serialize(Object o) throws ZkMarshallingError {
        return new byte[0];
      }

      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        System.out.println("Read the zNode, size: " + bytes.length);
        return bytes;
      }
    };
    HelixZkClient zkClient = DedicatedZkClientFactory.getInstance().buildZkClient(
        new HelixZkClient.ZkConnectionConfig(LOCAL_ZK),
        new HelixZkClient.ZkClientConfig().setZkSerializer(zkSerializer));
    try {
//      Stat stat = zkClient.getStat(LARGE_NODE_PATH);
      Object data = zkClient.readData(LARGE_NODE_PATH);
      System.out.println("Read the data content: " + data);
    } catch (Exception e) {
      System.out.println("Error while creating the node" + e.getMessage());
    } finally {
//      zkClient.delete(LARGE_NODE_PATH);
      zkClient.close();
    }
  }
}
