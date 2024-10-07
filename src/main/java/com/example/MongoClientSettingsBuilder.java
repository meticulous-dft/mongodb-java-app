package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;

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
            builder -> builder.addConnectionPoolListener(new CustomConnectionPoolListener()))
        .addCommandListener(new CustomCommandListener())
        .retryWrites(true)
        .retryReads(true)
        .build();
  }
}
