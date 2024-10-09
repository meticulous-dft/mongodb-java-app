package com.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBScalingTest {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBScalingTest.class);

  public static void main(String[] args) {
    Config config = Config.fromEnv();

    if (args.length > 0 && args[0].equals("load")) {
      logger.info("Starting data loading phase");
      loadData(config);
    } else {
      logger.info("Starting load testing phase");
      runLoadTest(config);
    }
  }

  private static void loadData(Config config) {
    try (MongoClient mongoClient =
        MongoClients.create(MongoClientSettingsBuilder.build(config.getConnectionString()))) {
      MongoDatabase database = mongoClient.getDatabase(config.getDatabaseName());
      MongoCollection<Document> collection = database.getCollection(config.getCollectionName());

      ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());

      long totalDocuments = (long) config.getNumThreads() * config.getDocumentsPerThread();
      logger.info("Preparing to insert {} documents", totalDocuments);

      AtomicLong insertedDocuments = new AtomicLong(0);

      long startTime = System.currentTimeMillis();

      // Create index first
      DataLoader indexCreator =
          new DataLoader(
              collection,
              0,
              0,
              insertedDocuments,
              totalDocuments,
              config.getTargetDocumentSize(),
              -1);
      indexCreator.createIndexIfNeeded();

      for (int i = 0; i < config.getNumThreads(); i++) {
        int startIndex = i * config.getDocumentsPerThread();
        executor.submit(
            new DataLoader(
                collection,
                config.getDocumentsPerThread(),
                startIndex,
                insertedDocuments,
                totalDocuments,
                config.getTargetDocumentSize(),
                i));
      }

      executor.shutdown();

      // Start a progress logging thread
      Thread progressLogger =
          new Thread(
              () -> {
                while (!executor.isTerminated()) {
                  long inserted = insertedDocuments.get();
                  double percentage = (inserted * 100.0) / totalDocuments;
                  logger.info(
                      "Overall Progress: {} / {} documents inserted ({}%)",
                      inserted, totalDocuments, String.format("%.2f", percentage));
                  try {
                    Thread.sleep(10000); // Log every 10 seconds
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              });
      progressLogger.start();

      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      progressLogger.interrupt(); // Stop the progress logger
      progressLogger.join();

      long endTime = System.currentTimeMillis();
      long duration = (endTime - startTime) / 1000; // in seconds

      logger.info(
          "Data loading completed. {} documents inserted. Duration: {} seconds",
          insertedDocuments.get(),
          duration);
    } catch (InterruptedException e) {
      logger.error("Data loading interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private static void runLoadTest(Config config) {
    MetricsManager metricsManager = MetricsManager.getInstance();
    metricsManager.resetStartTime();

    try (MongoClient mongoClient =
        MongoClients.create(MongoClientSettingsBuilder.build(config.getConnectionString()))) {
      MongoDatabase database = mongoClient.getDatabase(config.getDatabaseName());
      MongoCollection<Document> collection = database.getCollection(config.getCollectionName());

      ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());

      for (int i = 0; i < config.getNumThreads(); i++) {
        executor.submit(
            new OperationWorker(
                collection,
                config.getDocumentsPerThread(),
                config.getWritePercentage(),
                config.getTargetDocumentSize()));
      }

      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

      logger.info("Load test completed.");
      metricsManager.printCurrentMetrics(); // Print final metrics
    } catch (InterruptedException e) {
      logger.error("Load test interrupted", e);
      Thread.currentThread().interrupt();
    }
  }
}
