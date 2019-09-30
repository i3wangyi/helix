package org.apache.helix.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;
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

  private static List<String> getTrainingDataSet(float[] weights, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> initPossibleAssignment = clusterModel.getContext().getBestPossibleAssignment();

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences = Collections.emptyMap();

    RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

    // create a list of new nodes
    List<AssignableNode> newNodes =
        MockClusterModelBuilder.createInstances("NewInstance", 10, "NewZone", ImmutableMap.of("size", 500), 30);

    // add these new nodes to the cluster
    clusterModel.onClusterExpansion(newNodes);
    OptimalAssignment clusterExpansionOptimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    double clusterExpansionEvenness = clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    double clusterExpansionMaxUsage = clusterModel.getMaxCapacityKeyUsageAsEvenness();
    // TODO: check if there're movements between existing nodes
    double clusterExpansionMovements =
        clusterModel.getTotalMovedPartitionsCount(clusterExpansionOptimalAssignment, initPossibleAssignment)
            / totalPartitionsCount;
    // remove the newly added nodes
    clusterModel.onInstanceCrash(newNodes);
    OptimalAssignment instanceCrashOptimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    double instanceCrashEvenness = clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    double instanceCrashMaxUsage = clusterModel.getMaxCapacityKeyUsageAsEvenness();
    double instanceCrashMovements =
        clusterModel.getTotalMovedPartitionsCount(instanceCrashOptimalAssignment, initPossibleAssignment)
            / totalPartitionsCount;

    List<String> rows = new ArrayList<>();
    for (float weight : weights) {
      rows.add(String.valueOf(weight));
    }
    rows.add(String.valueOf(clusterExpansionEvenness));
    rows.add(String.valueOf(clusterExpansionMovements));
    rows.add(String.valueOf(clusterExpansionMaxUsage));
    rows.add(String.valueOf(instanceCrashEvenness));
    rows.add(String.valueOf(instanceCrashMovements));
    rows.add(String.valueOf(instanceCrashMaxUsage));

    return rows;
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster").setZoneCount(3)
        .setInstanceCountPerZone(10)
        .setResourceCount(3)
        .setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10)
        .build();

    List<List<String>> result = new ArrayList<>();

    for (int r = 0; r < 1000; r++) {
      clusterModel = new MockClusterModel(clusterModel);
      List<Float> settings = randomGenerateConfigs(5);
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences = Collections.emptyMap();
      float[] weights = getPrimitives(settings);
      RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

      OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
      clusterModel.getContext().setBaselineAssignment(bestPossibleAssignment);

      result.add(getTrainingDataSet(weights, clusterModel));
    }

    List<String> names = ImmutableList.of("PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
        "ResourceTopStateCount", "MaxCapacityKeyUsage", "clusterExpansionEvenness", "clusterExpansionMovements",
        "clusterExpansionMaxUsage", "instanceCrashEvenness", "instanceCrashMovements", "instanceCrashMaxUsage");
    writeToCSV("dataset.csv", names, result);
  }
}
