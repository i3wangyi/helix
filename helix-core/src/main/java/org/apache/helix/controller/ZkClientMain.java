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

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.manager.zk.ZkClient;


public class ZkClientMain {
  public static void main(String[] args) {
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
