package org.apache.helix.experiment;

import com.google.common.collect.Lists;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.zookeeper.Op;


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

  private static List<Float> simulate(RebalanceAlgorithm rebalanceAlgorithm, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> initPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment();
    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    float evenness = (float) (double) clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    float movements = (float) clusterModel.getTotalMovedPartitionsCount(optimalAssignment, initPossibleAssignment)
        / totalPartitionsCount;

    return ImmutableList.of(evenness, movements);
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
      Map<String, ResourceAssignment> initBestAssignment =
          initAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(initBestAssignment);
      clusterModel.getContext().setBaselineAssignment(initBestAssignment);
      // create a list of new nodes
      List<AssignableNode> newNodes =
          MockClusterModelBuilder.createInstances("NewInstance", 40, "NewZone", ImmutableMap.of("size", 1000), 30);
      // add these new nodes to the cluster
      clusterModel.onClusterExpansion(newNodes);

      numbers.addAll(simulate(algorithm, clusterModel));
      result.add(numbers.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    List<String> names = ImmutableList.of("PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
        "ResourceTopStateCount", "MaxCapacityKeyUsage", "evenness", "movements");
    writeToCSV("dataset.csv", names, result);
  }
}
