package com.example;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import java.util.Random;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationWorker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger("com.example.LoadTest");
  private static final Random RANDOM = new Random();
  private final MongoCollection<Document> collection;
  private final int operationsCount;
  private final int writePercentage;
  private final MetricsManager metricsManager;
  private final int targetDocumentSize;

  public OperationWorker(
      MongoCollection<Document> collection,
      int operationsCount,
      int writePercentage,
      int targetDocumentSize) {
    this.collection = collection;
    this.operationsCount = operationsCount;
    this.writePercentage = writePercentage;
    this.metricsManager = MetricsManager.getInstance();
    this.targetDocumentSize = targetDocumentSize;
  }

  @Override
  public void run() {
    for (int i = 0; i < operationsCount; i++) {
      try {
        if (RANDOM.nextInt(100) < writePercentage) {
          performWrite();
        } else {
          performRead();
        }
        metricsManager.incrementTotalOperations();

        if (i % 100 == 0 && i > 0) {
          logger.info("{} completed {} operations", Thread.currentThread().getName(), i);
        }
      } catch (MongoException e) {
        logger.warn("Operation failed: {}", e.getMessage(), e);
        metricsManager.incrementFailedOperations();
      }
    }
  }

  private void performWrite() {
    int randomId = RANDOM.nextInt(operationsCount);
    Document updateDoc = DocumentGenerator.generateRichDocument(randomId, targetDocumentSize);
    Bson filter = new Document("index", randomId);
    Bson update =
        Updates.combine(
            Updates.set("timestamp", updateDoc.getLong("timestamp")),
            Updates.set("user", updateDoc.get("user")),
            Updates.set("order", updateDoc.get("order")),
            Updates.set("metadata", updateDoc.get("metadata")));
    UpdateOptions options = new UpdateOptions().upsert(true);

    long startTime = System.nanoTime();
    collection.updateOne(filter, update, options);
    long endTime = System.nanoTime();
    double latencyMs = (endTime - startTime) / 1_000_000.0;
    metricsManager.recordWriteLatency(latencyMs);
    metricsManager.incrementWriteOperations();
    logger.debug("Updated document with index: {}", randomId);
  }

  private void performRead() {
    int randomId = RANDOM.nextInt(operationsCount);
    long startTime = System.nanoTime();
    Document result = collection.find(new Document("index", randomId)).first();
    long endTime = System.nanoTime();
    double latencyMs = (endTime - startTime) / 1_000_000.0;
    metricsManager.recordReadLatency(latencyMs);
    metricsManager.incrementReadOperations();
    logger.debug(
        "Read document with index: {}",
        (result != null ? result.getInteger("index") : "not found"));
  }
}
