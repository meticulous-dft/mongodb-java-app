package com.example;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  // for stress testing
  private static final int STRESS_TEST_OPERATIONS_PER_THREAD = 1_000_000;
  private static final AtomicBoolean stressTestKeepRunning = new AtomicBoolean(true);

  public static void main(String[] args) {
    Config config = Config.fromEnv();

    if (args.length > 0 && args[0].equals("load")) {
      logger.info("Starting data loading phase");
      loadData(config);
    } else if (args.length > 0 && args[0].equals("stress")) {
      logger.info("Starting stress testing phase");
      runStressTest(config);
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

      // Setup index
      setupIndex(config, mongoClient);

      ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());

      long totalDocuments = (long) config.getNumThreads() * config.getDocumentsPerThread();
      logger.info("Preparing to insert {} documents", totalDocuments);

      AtomicLong insertedDocuments = new AtomicLong(0);

      long startTime = System.currentTimeMillis();

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

      // Start a progress logging thread
      long totalDocuments = (long) config.getNumThreads() * config.getDocumentsPerThread();
      Thread progressLogger =
          new Thread(
              () -> {
                while (!executor.isTerminated()) {
                  metricsManager.printCurrentMetrics();
                  long totalOps = metricsManager.getTotalOperationsCount().get();
                  double percentage = (totalOps * 100.0) / totalDocuments;
                  logger.info(
                      "Overall Progress: {} / {} documents ({}%)",
                      totalOps, totalDocuments, String.format("%.2f", percentage));
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

      logger.info("Load test completed.");
      metricsManager.printCurrentMetrics(); // Print final metrics
    } catch (InterruptedException e) {
      logger.error("Load test interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private static boolean isCollectionCreated(MongoClient client, String dbName, String collName) {
    MongoCursor<String> collections = client.getDatabase(dbName).listCollectionNames().iterator();
    while (collections.hasNext()) {
      String c = collections.next();
      if (c.equals(collName)) {
        return true;
      }
    }
    return false;
  }

  private static void setupIndex(Config config, MongoClient client) {
    try {
      if (!isCollectionCreated(client, config.getDatabaseName(), config.getCollectionName())) {
        client
            .getDatabase(config.getDatabaseName())
            .createCollection(config.getCollectionName(), new CreateCollectionOptions());
      }

      if (!config.sharded()) {
        logger.info("Creating index on 'index' field");
        client
            .getDatabase(config.getDatabaseName())
            .getCollection(config.getCollectionName())
            .createIndex(Indexes.ascending("index"), new IndexOptions().background(true));
        logger.info("Index creation completed");
        return;
      }

      // Enable sharding for the database
      Document enableSharding = new Document("enableSharding", config.getDatabaseName());
      client.getDatabase("admin").runCommand(enableSharding);

      // Shard the collection
      Document shardCollection =
          new Document(
                  "shardCollection", config.getDatabaseName() + "." + config.getCollectionName())
              .append("key", new Document("index", "hashed"));
      client.getDatabase("admin").runCommand(shardCollection);

      logger.info(
          "Sharding setup completed for collection {}.{} with shard key: {}",
          config.getDatabaseName(),
          config.getCollectionName(),
          "index");
    } catch (MongoCommandException e) {
      if (e.getErrorCode() == 23) {
        // Already sharded, ignore
        logger.info("Collection is already sharded");
      } else if (e.getErrorCode() == 85) {
        // Index already exists, ignore
        logger.info("Index already exists");
      } else {
        throw e;
      }
    }
  }

  private static void runStressTest(Config config) {
    try (MongoClient mongoClient =
        MongoClients.create(MongoClientSettingsBuilder.build(config.getConnectionString()))) {
      MongoDatabase database = mongoClient.getDatabase(config.getDatabaseName());
      MongoCollection<Document> collection = database.getCollection(config.getCollectionName());

      ExecutorService executor = Executors.newFixedThreadPool(config.getNumThreads());
      for (int i = 0; i < config.getNumThreads(); i++) {
        executor.submit(new CPUIntensiveTask());
      }
      // Start MongoDB operations
      ExecutorService dbOperationsExecutor = Executors.newSingleThreadExecutor();
      dbOperationsExecutor.submit(() -> performMongoDBOperations(collection));

      // Wait for 60 minutes
      Thread.sleep(60_000 * 60);

      // Stop all tasks
      stressTestKeepRunning.set(false);

      // Shutdown the executors and wait for tasks to complete
      executor.shutdown();
      dbOperationsExecutor.shutdown();
      executor.awaitTermination(1, TimeUnit.MINUTES);
      dbOperationsExecutor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.error("Stress test interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private static void performMongoDBOperations(MongoCollection<Document> collection) {
    int operationCount = 0;
    while (stressTestKeepRunning.get() && operationCount < STRESS_TEST_OPERATIONS_PER_THREAD) {
      try {
        Document doc = new Document("testField", "testValue" + operationCount);
        collection.insertOne(doc);

        // Simulate some read operations as well
        Document result =
            collection.find(new Document("testField", "testValue" + operationCount)).first();
        if (result == null) {
          logger.error("Read operation failed for document {}", operationCount);
        }

        operationCount++;
        if (operationCount % 100 == 0) {
          logger.info("Completed {} operations", operationCount);
        }
      } catch (Exception e) {
        logger.error("Error during MongoDB operation: {}", e.getMessage());
      }
    }
  }

  static class CPUIntensiveTask implements Runnable {
    @Override
    public void run() {
      logger.info("stressing cpu");
      List<Integer> primes = new ArrayList<>();
      while (stressTestKeepRunning.get()) {
        for (int i = 2; i < 1_000_000_000; i++) {
          if (i % 10_000_000 == 0) {
            logger.info("checking {} is prime", i);
          }
          if (isPrime(i)) {
            primes.add(i);
          }
        }
        primes.clear(); // Clear to avoid excessive memory usage
      }
    }

    private boolean isPrime(int n) {
      if (n <= 1) return false;
      for (int i = 2; i <= Math.sqrt(n); i++) {
        if (n % i == 0) return false;
      }
      return true;
    }
  }
}
