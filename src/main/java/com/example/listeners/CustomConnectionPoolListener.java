package com.example.listeners;

import com.mongodb.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomConnectionPoolListener implements ConnectionPoolListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomConnectionPoolListener.class);

  @Override
  public void connectionPoolOpened(ConnectionPoolOpenedEvent event) {
    logger.debug("Connection pool opened: {}", event.getServerId());
  }

  @Override
  public void connectionPoolClosed(ConnectionPoolClosedEvent event) {
    logger.debug("Connection pool closed: {}", event.getServerId());
  }

  @Override
  public void connectionCheckedOut(ConnectionCheckedOutEvent event) {
    logger.debug("Connection checked out: {}", event.getConnectionId());
  }

  @Override
  public void connectionCheckedIn(ConnectionCheckedInEvent event) {
    logger.debug("Connection checked in: {}", event.getConnectionId());
  }

  @Override
  public void connectionCreated(ConnectionCreatedEvent event) {
    logger.debug("Connection created: {}", event.getConnectionId());
  }

  @Override
  public void connectionReady(ConnectionReadyEvent event) {
    logger.debug("Connection ready: {}", event.getConnectionId());
  }

  @Override
  public void connectionClosed(ConnectionClosedEvent event) {
    logger.debug("Connection closed: {}", event.getConnectionId());
  }
}
