package org.apache.helix.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
  private static List<Float> randomGenerateConfigs(int size) {
    List<Float> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add((float) new Random().nextInt(100));
    }

    return result;
  }

  private static void writeToCSV(String fileName, List<String> columns, List<List<String>> rows) throws IOException {

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

  private static Map<String, Double> simulate(RebalanceAlgorithm rebalanceAlgorithm, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    Map<String, Double> stats = new HashMap<>();
    Map<String, ResourceAssignment> initPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment();
    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    double stdEvenness = clusterModel.getCoefficientOfVariation().get("size");
    stats.put("stdEvenness", stdEvenness);
    double partitionMovements = clusterModel.getTotalMovedPartitionsCount(optimalAssignment, initPossibleAssignment);
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
        .setInstanceCountPerZone(10)
        .setResourceCount(3)
        .setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10)
        .build();

    List<List<String>> result = new ArrayList<>();

    for (int i = 0; i < 1000; i++) {
      MockClusterModel clusterModel = reset(baseModel);
      List<Float> numbers = randomGenerateConfigs(5);
      float[] randomWeights = getPrimitives(numbers);
      RebalanceAlgorithm algorithm = getAlgorithm(randomWeights);
      OptimalAssignment initAssignment = algorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> initBestAssignment = initAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(initBestAssignment);
      clusterModel.getContext().setBaselineAssignment(initBestAssignment);
      // create a list of new nodes
      List<AssignableNode> newNodes =
          MockClusterModelBuilder.createInstances("NewInstance", 40, "NewZone", ImmutableMap.of("size", 1000), 30);
      // add these new nodes to the cluster
      clusterModel.onClusterExpansion(newNodes);

      Map<String, Double> stats = simulate(algorithm, clusterModel);
      //TODO: collect stats
      result.add(numbers.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    List<String> names = ImmutableList.of("PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
        "ResourceTopStateCount", "MaxCapacityKeyUsage", "evenness", "movements");
    writeToCSV("dataset-40.csv", names, result);
  }
}
