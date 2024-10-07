package com.example;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import java.util.Random;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationWorker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OperationWorker.class);
  private static final Random RANDOM = new Random();
  private final MongoCollection<Document> collection;
  private final int operationsCount;
  private final int writePercentage;
  private final MetricsManager metricsManager;

  public OperationWorker(
      MongoCollection<Document> collection, int operationsCount, int writePercentage) {
    this.collection = collection;
    this.operationsCount = operationsCount;
    this.writePercentage = writePercentage;
    this.metricsManager = MetricsManager.getInstance();
  }

  @Override
  public void run() {
    for (int i = 0; i < operationsCount; i++) {
      try {
        if (RANDOM.nextInt(100) < writePercentage) {
          performWrite(i);
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

  private void performWrite(int index) {
    Document doc = DocumentGenerator.generateRichDocument(index);
    long startTime = System.nanoTime();
    collection.insertOne(doc);
    long endTime = System.nanoTime();
    double latencyMs = (endTime - startTime) / 1_000_000.0;
    metricsManager.recordWriteLatency(latencyMs);
    metricsManager.incrementWriteOperations();
    logger.debug(
        "Inserted document with index: {}, size: {} bytes",
        index,
        DocumentGenerator.calculateSize(doc));
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
