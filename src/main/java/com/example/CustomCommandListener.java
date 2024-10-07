package com.example;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCommandListener implements CommandListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomCommandListener.class);

  @Override
  public void commandStarted(CommandStartedEvent event) {
    logger.debug("Command started: {}", event.getCommandName());
  }

  @Override
  public void commandSucceeded(CommandSucceededEvent event) {
    logger.debug(
        "Command succeeded: {}, took {} ms",
        event.getCommandName(),
        event.getElapsedTime(TimeUnit.MILLISECONDS));
  }

  @Override
  public void commandFailed(CommandFailedEvent event) {
    logger.warn(
        "Command failed: {}, error: {}", event.getCommandName(), event.getThrowable().getMessage());
  }
}
