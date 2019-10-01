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

  private static List<Double> simulate(RebalanceAlgorithm rebalanceAlgorithm, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> initPossibleAssignment =
        clusterModel.getContext().getBestPossibleAssignment();
    OptimalAssignment optimalAssignment =
        rebalanceAlgorithm.calculate(clusterModel);
    double evenness =
        clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    double movements =
        clusterModel.getTotalMovedPartitionsCount(optimalAssignment, initPossibleAssignment) / totalPartitionsCount;

    return ImmutableList.of(evenness, movements);
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    MockClusterModel baseModel = new MockClusterModelBuilder("TestCluster").setZoneCount(3)
        .setInstanceCountPerZone(10).setResourceCount(3).setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10).build();

    List<List<String>> result = new ArrayList<>();

    // generate cluster expansion data set
    for (int i = 0; i < 1000; i++) {
      // make sure nodes are not assigned anything
      baseModel.getAssignableNodes().forEach(AssignableNode::releaseAll);
      MockClusterModel clusterModel = new MockClusterModel(baseModel);
      List<Float> settings = randomGenerateConfigs(5);
      float[] weights = getPrimitives(settings);
      RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(Collections.emptyMap(), weights);
      OptimalAssignment initAssignment = rebalanceAlgorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> bestPossibleAssignment =
          initAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
      clusterModel.getContext().setBaselineAssignment(bestPossibleAssignment);

      // create a list of new nodes
      List<AssignableNode> newNodes = MockClusterModelBuilder.createInstances("NewInstance", 10,
          "NewZone", ImmutableMap.of("size", 1000), 30);
      // add these new nodes to the cluster
      clusterModel.onClusterExpansion(newNodes);
      List<String> rows = new ArrayList<>();
      for (float weight : weights) {
        rows.add(String.valueOf(weight));
      }
      List<Double> r = simulate(rebalanceAlgorithm, clusterModel);
      String evenness = String.valueOf(r.get(0));
      String movements = String.valueOf(r.get(1));

      rows.add(evenness);
      rows.add(movements);
      result.add(rows);
    }

    List<String> names =
        ImmutableList.of("PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
            "ResourceTopStateCount", "MaxCapacityKeyUsage", "evenness", "movements");
    writeToCSV("dataset.csv", names, result);
  }
}
