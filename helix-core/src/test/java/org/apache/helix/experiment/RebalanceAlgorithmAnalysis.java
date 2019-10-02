package org.apache.helix.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RebalanceAlgorithmAnalysis {
  private static void writeToCSV(String fileName, List<String> columns, List<List<String>> rows)
      throws IOException {

    FileWriter csvWriter = new FileWriter(fileName);
    csvWriter.append(String.join(",", columns)).append("\n");
    for (List<String> row : rows) {
      csvWriter.append(String.join(",", row)).append("\n");
    }
    csvWriter.flush();
    csvWriter.close();
  }

  private static float[] getPrimitives(List<Float> values) {
    float[] r = new float[values.size()];
    for (int i = 0; i < values.size(); i++) {
      r[i] = values.get(i);
    }
    return r;
  }

  private static LinkedHashMap<String, Double> simulateAndGetStat(
      RebalanceAlgorithm rebalanceAlgorithm, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    LinkedHashMap<String, Double> stats = new LinkedHashMap<>();
    Map<String, ResourceAssignment> initPossibleAssignment =
        clusterModel.getContext().getBestPossibleAssignment();
    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    double cvEvenness = clusterModel.getCoefficientOfVariation().get("size");
    stats.put("evennessCV", cvEvenness);
    double partitionMovements =
        clusterModel.getTotalMovedPartitionsCount(optimalAssignment, initPossibleAssignment);
    stats.put("partitionMovement", partitionMovements);
    double maxResourceCount = clusterModel.getMaxResourceCountAsEvenness();
    stats.put("maxResourceCount", maxResourceCount);
    double maxPartitionCount = clusterModel.getMaxPartitionsCountAsEvenness();
    stats.put("maxPartitionCount", maxPartitionCount);
    double maxCapacityKeyUsage = clusterModel.getMaxCapacityKeyUsageAsEvenness();
    stats.put("maxCapacityKeyUsage", maxCapacityKeyUsage);
    double maxTopStateCount = clusterModel.getMaxTopStatesAsEvenness();
    stats.put("maxTopStateUsage", maxTopStateCount);

    return stats;
  }

  private static MockClusterModel reset(MockClusterModel baseModel) {
    baseModel.getAssignableNodes().forEach(AssignableNode::releaseAll);
    return new MockClusterModel(baseModel);
  }

  private static RebalanceAlgorithm getAlgorithm(float[] weights) {
    return ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap(), weights);
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    MockClusterModel baseModel = new MockClusterModelBuilder("TestCluster").setZoneCount(4)
        .setInstanceCountPerZone(10).setResourceCount(3).setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10).build();

    List<List<String>> result = new ArrayList<>();
    List<String> features = ImmutableList.of("PartitionMovement", "InstancePartitionCount",
        "ResourcePartitionCount", "ResourceTopStateCount", "MaxCapacityKeyUsage");
    List<String> columnNames = new ArrayList<>(features);

    for (int i = 0; i < 100; i++) {
      MockClusterModel clusterModel = reset(baseModel);
      Map<String, Float> weights = features.stream().collect(
          Collectors.toMap(Function.identity(), name -> (float) new Random().nextInt(100)));

      float[] randomWeights = getPrimitives(new ArrayList<>(weights.values()));
      RebalanceAlgorithm algorithm = getAlgorithm(randomWeights);
      OptimalAssignment initAssignment = algorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> initBestAssignment =
          initAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(initBestAssignment);
      clusterModel.getContext().setBaselineAssignment(initBestAssignment);
      // create a list of new nodes
      List<AssignableNode> newNodes = MockClusterModelBuilder.createInstances("NewInstance", 40,
          "NewZone", ImmutableMap.of("size", 1000), 30);
      // add these new nodes to the cluster
      clusterModel.onClusterExpansion(newNodes);

      LinkedHashMap<String, Double> stats = simulateAndGetStat(algorithm, clusterModel);
      if (columnNames.size() == 5) {
        columnNames.addAll(new ArrayList<>(stats.keySet()));
      }
      List<Float> numbers = new ArrayList<>(weights.values());
      numbers.addAll(
          stats.values().stream().map(val -> (float) (double) val).collect(Collectors.toList()));

      result.add(numbers.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    writeToCSV("dataset-40.csv", columnNames, result);
  }
}
