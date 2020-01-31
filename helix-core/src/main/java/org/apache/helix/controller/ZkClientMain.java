package org.apache.helix.controller;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
import org.testng.annotations.Test;


public class ZkClientMain {
  private static final String LOCAL_ZK = "localhost:2121";
  private static final String LARGE_NODE_PATH = "/test1MB";

  @Test
  public void testCreateLargeZnode() {
//    System.setProperty("jute.maxbuffer", String.valueOf(1024 * 1024 * 2L));
    System.setProperty("jute.maxbuffer", 4096 * 1024 * 10 + "");
    byte[] bytes = new byte[1024 * 1024];
    Arrays.fill(bytes, (byte) 1);
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
      Object data = zkClient.readData(LARGE_NODE_PATH);
    } catch (Exception e) {
      System.out.println("Error while creating the node" + e.getMessage());
    } finally {
      zkClient.delete(LARGE_NODE_PATH);
      zkClient.close();
    }
  }

  @Test
  public void test() {
    ZkClient zkClient = new ZkClient("localhost:2121");
    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

//    zkClient.subscribeDataChanges("/ABC/TEST", new IZkDataListener() {
//      @Override
//      public void handleDataChange(String s, Object o)
//          throws Exception {
//        System.out.println("Data Change: " + s);
//      }
//
//      @Override
//      public void handleDataDeleted(String s)
//          throws Exception {
//        System.out.println("Data deleted: " + s);
//      }
//    });

    zkClient.subscribeChildChanges("/ABC", new IZkChildListener() {
      @Override
      public void handleChildChange(String s, List<String> list)
          throws Exception {
        System.out.println("Path of child change: " + s);
        System.out.println("Child changes: " + list);
      }
    });

    try {
      lock.lock();
      System.out.println("Wait for condition");
      condition.await();
      lock.unlock();
      System.out.println("Condition meet");
    } catch (InterruptedException e) {

    } finally {
      zkClient.close();
    }
  }
}
