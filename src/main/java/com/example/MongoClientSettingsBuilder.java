package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import java.util.concurrent.TimeUnit;

public class MongoClientSettingsBuilder {
  public static MongoClientSettings build(String connectionString) {
    return MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .applyToClusterSettings(builder -> builder.addClusterListener(new CustomClusterListener()))
        .applyToServerSettings(
            builder ->
                builder
                    .addServerListener(new CustomServerListener())
                    .addServerMonitorListener(new CustomServerMonitorListener()))
        .applyToConnectionPoolSettings(
            builder -> {
              builder.addConnectionPoolListener(new CustomConnectionPoolListener());
              builder
                  .maxSize(100) // Increase max connections
                  .minSize(20) // Set min connections
                  .maxWaitTime(30000, TimeUnit.MILLISECONDS) // Max wait time for a connection
                  .maxConnectionLifeTime(1, TimeUnit.HOURS); // Max connection lifetime
            })
        .addCommandListener(new CustomCommandListener())
        .retryWrites(true)
        .retryReads(true)
        .build();
  }
}
