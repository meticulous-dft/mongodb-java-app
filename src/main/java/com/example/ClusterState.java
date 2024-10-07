package com.example;

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterState {
  private static final ClusterState INSTANCE = new ClusterState();

  private volatile String topologyType;
  private volatile List<String> serverAddresses;
  private volatile String primaryAddress;
  private volatile boolean isWritable;

  private ClusterState() {}

  public static ClusterState getInstance() {
    return INSTANCE;
  }

  public void updateState(ClusterDescription description) {
    this.topologyType = description.getType().name();
    this.serverAddresses =
        description.getServerDescriptions().stream()
            .map(sd -> sd.getAddress().toString())
            .collect(Collectors.toList());
    this.primaryAddress =
        description.getServerDescriptions().stream()
            .filter(ServerDescription::isPrimary)
            .findFirst()
            .map(sd -> sd.getAddress().toString())
            .orElse(null);
    this.isWritable = description.hasWritableServer();
  }

  public String getTopologyType() {
    return topologyType;
  }

  public List<String> getServerAddresses() {
    return serverAddresses;
  }

  public String getPrimaryAddress() {
    return primaryAddress;
  }

  public boolean isWritable() {
    return isWritable;
  }

  @Override
  public String toString() {
    return String.format(
        "Topology: %s, Servers: %s, Primary: %s, Writable: %s",
        topologyType, serverAddresses, primaryAddress, isWritable);
  }
}
