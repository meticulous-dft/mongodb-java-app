package com.example;

import com.mongodb.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomServerListener implements ServerListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomServerListener.class);

  @Override
  public void serverOpening(ServerOpeningEvent event) {
    logger.info("Server opening - Server: {}", event.getServerId());
  }

  @Override
  public void serverClosed(ServerClosedEvent event) {
    logger.info("Server closed - Server: {}", event.getServerId());
  }

  @Override
  public void serverDescriptionChanged(ServerDescriptionChangedEvent event) {
    logger.info(
        "Server description changed - Server: {}, New description: {}",
        event.getServerId(),
        event.getNewDescription());
    logger.debug("Previous description: {}", event.getPreviousDescription());
  }
}
