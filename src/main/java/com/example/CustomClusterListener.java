package com.example;

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.*;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomClusterListener implements ClusterListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomClusterListener.class);
  private final ClusterState clusterState = ClusterState.getInstance();

  @Override
  public void clusterOpening(ClusterOpeningEvent event) {
    logger.info("Cluster opening - Cluster ID: {}", event.getClusterId());
  }

  @Override
  public void clusterClosed(ClusterClosedEvent event) {
    logger.info("Cluster closed - Cluster ID: {}", event.getClusterId());
  }

  @Override
  public void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
    ClusterDescription newDescription = event.getNewDescription();
    clusterState.updateState(newDescription);

    logger.info("Cluster description changed - Cluster ID: {}", event.getClusterId());
    logger.info("New cluster state: {}", clusterState);

    // Log detailed changes if there's a difference
    if (!event.getPreviousDescription().equals(newDescription)) {
      logger.info("Topology changed from {} to {}",
          event.getPreviousDescription().getType(),
          newDescription.getType());

      Optional<ServerDescription> oldPrimary = event.getPreviousDescription().getServerDescriptions().stream()
          .filter(ServerDescription::isPrimary)
          .findFirst();
      Optional<ServerDescription> newPrimary = newDescription.getServerDescriptions().stream()
          .filter(ServerDescription::isPrimary)
          .findFirst();

      if (!oldPrimary.equals(newPrimary)) {
        logger.info("Primary changed from {} to {}",
            oldPrimary.map(sd -> sd.getAddress().toString()).orElse("none"),
            newPrimary.map(sd -> sd.getAddress().toString()).orElse("none"));
      }

      if (event.getPreviousDescription().hasWritableServer() != newDescription.hasWritableServer()) {
        logger.info("Cluster writability changed from {} to {}",
            event.getPreviousDescription().hasWritableServer(),
            newDescription.hasWritableServer());
      }
    }
  }
}