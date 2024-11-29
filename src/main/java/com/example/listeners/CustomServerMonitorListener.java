package com.example.listeners;

import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomServerMonitorListener implements ServerMonitorListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomServerMonitorListener.class);

  @Override
  public void serverHearbeatStarted(ServerHeartbeatStartedEvent event) {
    logger.debug("Starting heartbeat on {}", event.getConnectionId().getServerId().getAddress());
  }

  @Override
  public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
    logger.debug(
        "Server heartbeat succeeded: {}, took {} ms",
        event.getConnectionId().getServerId().getAddress(),
        event.getElapsedTime(TimeUnit.MILLISECONDS));
  }

  @Override
  public void serverHeartbeatFailed(ServerHeartbeatFailedEvent event) {
    logger.warn(
        "Server heartbeat failed: {}, error: {}",
        event.getConnectionId().getServerId().getAddress(),
        event.getThrowable().getMessage());
  }
}
