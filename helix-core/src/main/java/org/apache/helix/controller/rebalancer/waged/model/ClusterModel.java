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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;


/**
 * This class wraps the required input for the rebalance algorithm.
 */
public class ClusterModel {
  private ClusterContext _clusterContext;
  // Map to track all the assignable replications. <Resource Name, Set<Replicas>>
  private Map<String, Set<AssignableReplica>> _unAssignableReplicaMap;
  // The index to find the replication information with a certain state. <Resource, <Key(resource_partition_state), Replica>>
  // Note that the identical replicas are deduped in the index.
  private Map<String, Map<String, AssignableReplica>> _assignableReplicaIndex;
  // All available nodes to assign replicas
  @Deprecated
  private Map<String, AssignableNode> _assignableNodeMap;
  private Set<AssignableNode> _assignableNodes;
  private Set<AssignableReplica> _unAssignedReplicas;

  /**
   * @param clusterContext         The initialized cluster context.
   * @param unAssignedReplicas     The replicas to be assigned.
   *                               Note that the replicas in this list shall not be included while initializing the context and assignable nodes.
   * @param assignableNodes        The active instances.
   */
  ClusterModel(ClusterContext clusterContext, Set<AssignableReplica> unAssignedReplicas,
      Set<AssignableNode> assignableNodes) {
    reset(unAssignedReplicas, assignableNodes, clusterContext);
  }

  void reset(Set<AssignableReplica> unAssignedReplicas, Set<AssignableNode> assignableNodes,
      ClusterContext clusterContext) {
    // Save all the to be assigned replication
    _unAssignableReplicaMap = unAssignedReplicas.stream()
        .collect(Collectors.groupingBy(AssignableReplica::getResourceName, Collectors.toSet()));

    _unAssignedReplicas = new HashSet<>(unAssignedReplicas);
    _assignableNodes = new HashSet<>(assignableNodes);
    // reset the cluster context as well
    _clusterContext = new ClusterContext(clusterContext.getAllReplicas(), assignableNodes.size(),
        clusterContext.getBaselineAssignment(), clusterContext.getBestPossibleAssignment());
    // Index all the replicas to be assigned. Dedup the replica if two instances have the same resource/partition/state
    _assignableReplicaIndex = unAssignedReplicas.stream()
        .collect(Collectors.groupingBy(AssignableReplica::getResourceName,
            Collectors.toMap(AssignableReplica::toString, replica -> replica, (oldValue, newValue) -> oldValue)));

    _assignableNodeMap =
        assignableNodes.stream().collect(Collectors.toMap(AssignableNode::getInstanceName, node -> node));
  }

  public ClusterContext getContext() {
    return _clusterContext;
  }

  public Map<String, AssignableNode> getAssignableNodesAsMap() {
    return _assignableNodeMap;
  }

  public Set<AssignableNode> getAssignableNodes() {
    return _assignableNodes;
  }

  public Map<String, Set<AssignableReplica>> getAssignableReplicaMap() {
    return _unAssignableReplicaMap;
  }

  public void assign(AssignableNode node, AssignableReplica replica) {
    if (_assignableNodes.contains(node) && _unAssignedReplicas.contains(replica)) {
      if (!node.hasAssigned(replica)) {
        node.assign(replica);
        _clusterContext.addPartitionToFaultZone(node.getFaultZone(), replica.getResourceName(),
            replica.getPartitionName());
      }
    }
  }

  public void release(AssignableNode node, AssignableReplica replica) {
    if (_assignableNodes.contains(node) && _unAssignedReplicas.contains(replica)) {
      if (node.hasAssigned(replica)) {
        node.release(replica);
        _clusterContext.removePartitionFromFaultZone(node.getFaultZone(), replica.getResourceName(),
            replica.getPartitionName());
      }
    }
  }

  public List<AssignableReplica> getUnassignedReplicas() {
    return new ArrayList<>(_unAssignedReplicas);
  }

  /**
   * Assign the given replica to the specified instance and record the assignment in the cluster model.
   * The cluster usage information will be updated accordingly.
   *
   * @param resourceName
   * @param partitionName
   * @param state
   * @param instanceName
   */
  @Deprecated
  public void assign(String resourceName, String partitionName, String state, String instanceName) {
    AssignableNode node = locateAssignableNode(instanceName);
    AssignableReplica replica = locateAssignableReplica(resourceName, partitionName, state);

    node.assign(replica);
    _clusterContext.addPartitionToFaultZone(node.getFaultZone(), resourceName, partitionName);
  }

  /**
   * Revert the proposed assignment from the cluster model.
   * The cluster usage information will be updated accordingly.
   *
   * @param resourceName
   * @param partitionName
   * @param state
   * @param instanceName
   */
  @Deprecated
  public void release(String resourceName, String partitionName, String state, String instanceName) {
    AssignableNode node = locateAssignableNode(instanceName);
    AssignableReplica replica = locateAssignableReplica(resourceName, partitionName, state);

    node.release(replica);
    _clusterContext.removePartitionFromFaultZone(node.getFaultZone(), resourceName, partitionName);
  }

  private AssignableNode locateAssignableNode(String instanceName) {
    AssignableNode node = _assignableNodeMap.get(instanceName);
    if (node == null) {
      throw new HelixException("Cannot find the instance: " + instanceName);
    }
    return node;
  }

  private AssignableReplica locateAssignableReplica(String resourceName, String partitionName, String state) {
    AssignableReplica sampleReplica = _assignableReplicaIndex.getOrDefault(resourceName, Collections.emptyMap())
        .get(AssignableReplica.generateReplicaKey(resourceName, partitionName, state));
    if (sampleReplica == null) {
      throw new HelixException(
          String.format("Cannot find the replication with resource name %s, partition name %s, state %s.", resourceName,
              partitionName, state));
    }
    return sampleReplica;
  }
}
