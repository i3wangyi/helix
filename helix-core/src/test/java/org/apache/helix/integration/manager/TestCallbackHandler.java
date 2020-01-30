package org.apache.helix.integration.manager;

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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.manager.zk.CallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TestCallbackHandler.class);
  private static final String CLUSTER_NAME = "TestCluster";

//  @BeforeClass
//  public void init() {
//    _gSetupTool.addCluster(CLUSTER_NAME, true);
//    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestResource", 10, "MasterSlave");
//  }

  @Test
  public void testAsyncSubscribe() {
    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    ClusterControllerManager controller =
        new ClusterControllerManager("localhost:2121", CLUSTER_NAME, "TestController");
    controller.syncStart();
    DedupEventProcessor processor = CallbackHandler.getSubscribeChangeEventProcessor();
    Assert.assertTrue(processor.isAlive());
    try {
      lock.lock();
      System.out.println("Wait for condition");
      condition.await();
      lock.unlock();
      System.out.println("Condition meet");
    } catch (InterruptedException e) {

    } finally {
      controller.syncStop();
    }
  }
}
