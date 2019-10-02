package org.apache.helix.controller.rebalancer.waged.model;

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

import static com.google.common.math.DoubleMath.mean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

public class MockClusterModel extends ClusterModel {
  private List<AssignableNode> _newInstances = new ArrayList<>();

  public MockClusterModel(ClusterContext clusterContext, Set<AssignableReplica> unAssignedReplicas,
      Set<AssignableNode> assignableNodes) {
    super(clusterContext, unAssignedReplicas, assignableNodes);
  }

  public MockClusterModel(MockClusterModel other) {
    this(other.getContext(), new HashSet<>(other.getUnassignedReplicas()),
        other.getAssignableNodes());
  }

  public void onClusterExpansion(List<AssignableNode> newNodes) {
    Set<AssignableNode> currentNodes = getAssignableNodes();
    currentNodes.forEach(AssignableNode::releaseAll);
    // add the new node
    currentNodes.addAll(newNodes);
    ClusterContext clusterContext = getContext();
    Set<AssignableReplica> allReplicas = clusterContext.getAllReplicas();
    ClusterContext update = new ClusterContext(allReplicas, currentNodes.size(),
        clusterContext.getBaselineAssignment(), clusterContext.getBestPossibleAssignment());

    reset(allReplicas, currentNodes, update);
    _newInstances = newNodes;
  }

  public void onInstanceCrash(List<AssignableNode> nodes) {
    Set<AssignableNode> currentNodes = getAssignableNodes();
    Set<AssignableNode> crashedNodes = new HashSet<>(nodes);
    currentNodes.removeAll(crashedNodes);
    Set<AssignableReplica> unAssignedReplicas = crashedNodes.stream()
        .map(AssignableNode::getAssignedReplicas).flatMap(Set::stream).collect(Collectors.toSet());
    crashedNodes.forEach(AssignableNode::releaseAll);

    ClusterContext clusterContext = getContext();
    Set<AssignableReplica> allReplicas = clusterContext.getAllReplicas();
    ClusterContext update = new ClusterContext(allReplicas, currentNodes.size(),
        clusterContext.getBaselineAssignment(), clusterContext.getBestPossibleAssignment());

    reset(unAssignedReplicas, currentNodes, update);
  }

  public LinkedHashMap<String, Double> getTotalMovedPartitionsCountClusterExpansion(
      Map<String, ResourceAssignment> newAssignment,
      Map<String, ResourceAssignment> baseAssignment) {
    LinkedHashMap<String, Double> moves = new LinkedHashMap<>();
    int movedToNewInstances = 0;
    int movedBetweenExistingInstances = 0;
    int stateChangeBetweenOldInstance = 0;
    Set<String> newInstanceNames =
        _newInstances.stream().map(AssignableNode::getInstanceName).collect(Collectors.toSet());
    for (String resource : newAssignment.keySet()) {
      final ResourceAssignment resourceAssignment = newAssignment.get(resource);
      ResourceAssignment prevResourceAssignment = baseAssignment.get(resource);
      for (Partition partition : resourceAssignment.getMappedPartitions()) {
        Map<String, String> thisInstanceToStates = resourceAssignment.getReplicaMap(partition);
        Map<String, String> prevInstanceToStates = prevResourceAssignment.getReplicaMap(partition);

        for (String instance : thisInstanceToStates.keySet()) {
          if (newInstanceNames.contains(instance)) {
            movedToNewInstances += 1;
          } else {
            if (!prevInstanceToStates.containsKey(instance)) {
              movedBetweenExistingInstances += 1;
            } else if (!prevInstanceToStates.get(instance).equals(thisInstanceToStates.get(instance))) {
              stateChangeBetweenOldInstance += 1;
            }
          }
        }
      }
    }

    moves.put("movedToNewInstances", (double) movedToNewInstances);
    moves.put("movedBetweenOldInstances", (double) movedBetweenExistingInstances);
    moves.put("STBetweenOldInstances", (double) stateChangeBetweenOldInstance);
    return moves;
  }

  /**
   * The coefficient of variation (CV) is a statistical measure of the dispersion of data points in
   * a data series around the mean.
   * The coefficient of variation represents the ratio of the standard deviation to the mean, and it
   * is a useful statistic for comparing the degree of variation from one data series to another,
   * even if the means are drastically different from one another.
   * It's used as a tool to evaluate the "evenness" of an partitions to instances assignment
   * @return a multi-dimension CV keyed by capacity key
   */
  public Map<String, Double> getCoefficientOfVariation() {
    List<AssignableNode> instances = new ArrayList<>(getAssignableNodesAsMap().values());
    Map<String, List<Integer>> usages = new HashMap<>();
    for (AssignableNode instance : instances) {
      Map<String, Integer> capacityUsage = instance.getCapacityUsage();
      for (String key : capacityUsage.keySet()) {
        usages.computeIfAbsent(key, k -> new ArrayList<>()).add(capacityUsage.get(key));
      }
    }

    return usages.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> getCoefficientOfVariation(e.getValue())));
  }

  public double getMaxResourceCountAsEvenness() {
    List<Integer> usages = new ArrayList<>();
    for (AssignableNode node : getAssignableNodes()) {
      int count = node.getAssignedPartitionsMap().values().size();
      usages.add(count);
    }

    return getCoefficientOfVariation(usages);
  }

  public double getMaxTopStatesAsEvenness() {
    List<Integer> usages = new ArrayList<>();
    for (AssignableNode node : getAssignableNodes()) {
      usages.add(node.getAssignedTopStatePartitionsCount());
    }

    return getCoefficientOfVariation(usages);
  }

  public double getMaxCapacityKeyUsageAsEvenness() {
    Set<AssignableNode> instances = getAssignableNodes();
    List<Float> usages = new ArrayList<>();
    for (AssignableNode instance : instances) {
      usages.add(instance.getHighestCapacityUtilization());
    }
    return getCV(usages);
  }

  public double getMaxPartitionsCountAsEvenness() {
    List<Integer> usages = new ArrayList<>();
    for (AssignableNode node : getAssignableNodes()) {
      usages.add(node.getAssignedReplicaCount());
    }

    return getCoefficientOfVariation(usages);
  }

  public Map<String, Double> getMinMaxDifference() {
    Set<AssignableNode> instances = getAssignableNodes();
    Map<String, List<Integer>> usages = new HashMap<>();
    for (AssignableNode instance : instances) {
      Map<String, Integer> capacityUsage = instance.getCapacityUsage();
      for (String key : capacityUsage.keySet()) {
        usages.computeIfAbsent(key, k -> new ArrayList<>()).add(capacityUsage.get(key));
      }
    }

    return usages.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getMinMaxDifferenceVariation(e.getValue())));
  }

  private static double getMinMaxDifferenceVariation(List<Integer> nums) {
    double max = nums.get(0);
    double min = nums.get(1);

    for (int i = 1; i < nums.size(); i++) {
      max = Math.max(max, nums.get(i));
      min = Math.min(min, nums.get(i));
    }

    return Math.abs(max - min);
  }

  private static double getCoefficientOfVariation(List<Integer> nums) {
    double sum = 0;
    double mean = mean(nums);
    for (int num : nums) {
      sum += Math.pow((num - mean), 2);
    }
    double std = Math.sqrt(sum / (nums.size() - 1));
    return std / mean;
  }

  private static double getCV(List<Float> nums) {
    double sum = 0;
    double mean = mean(nums);
    for (float num : nums) {
      sum += Math.pow((num - mean), 2);
    }
    double std = Math.sqrt(sum / (nums.size() - 1));
    return std / mean;
  }
}
