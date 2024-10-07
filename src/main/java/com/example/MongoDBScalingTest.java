package com.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBScalingTest {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBScalingTest.class);
  public static final int TARGET_DOCUMENT_SIZE = 1024; // 1KB target size

  public static void main(String[] args) {
    Config config = Config.fromEnv();
    logger.info("Starting MongoDB scaling test with {} GB of data", config.getTotalDataSizeGB());

    MetricsManager metricsManager = MetricsManager.getInstance();
    metricsManager.resetStartTime();

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(metricsManager::printCurrentMetrics, 10, 10, TimeUnit.SECONDS);

    try (MongoClient mongoClient =
        MongoClients.create(MongoClientSettingsBuilder.build(config.getConnectionString()))) {
      MongoDatabase database = mongoClient.getDatabase(config.getDatabaseName());
      MongoCollection<Document> collection = database.getCollection(config.getCollectionName());

      ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());

      for (int i = 0; i < config.getNumThreads(); i++) {
        executor.submit(
            new OperationWorker(
                collection, config.getDocumentsPerThread(), config.getWritePercentage()));
      }

      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

      logger.info("Test completed.");
      metricsManager.printCurrentMetrics(); // Print final metrics
    } catch (InterruptedException e) {
      logger.error("Execution interrupted", e);
      Thread.currentThread().interrupt();
    } finally {
      scheduler.shutdownNow();
    }
  }
}
