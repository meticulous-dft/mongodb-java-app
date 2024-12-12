package com.example;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);
  private static final int MAX_RETRIES = 5;
  private static final int RETRY_DELAY_MS = 1000;

  private final MongoCollection<Document> collection;
  private final int documentsToLoad;
  private final int startIndex;
  private final int targetDocumentSize;
  private final MetricsManager metricsManager;
  private final int threadId;

  public DataLoader(
      MongoCollection<Document> collection,
      int documentsToLoad,
      int startIndex,
      int targetDocumentSize,
      int threadId) {
    this.collection = collection;
    this.documentsToLoad = documentsToLoad;
    this.startIndex = startIndex;
    this.targetDocumentSize = targetDocumentSize;
    this.metricsManager = MetricsManager.getInstance();
    this.threadId = threadId;
  }

  @Override
  public void run() {
    loadDocuments();
  }

  private void loadDocuments() {
    List<Document> batch = new ArrayList<>();
    int retries = 0;
    for (int i = 0; i < documentsToLoad; i++) {
      batch.add(DocumentGenerator.generateRichDocument(startIndex + i, targetDocumentSize));

      if (batch.size() == 1000 || i == documentsToLoad - 1) {
        boolean inserted = false;
        while (!inserted && retries < MAX_RETRIES) {
          try {
            long startTime = System.nanoTime();
            collection.insertMany(batch);
            long endTime = System.nanoTime();
            double latencyMs = (endTime - startTime) / 1_000_000.0;

            // Record metrics
            metricsManager.recordWriteLatency(latencyMs);
            metricsManager.addTotalOperations(batch.size());
            metricsManager.addWriteOperations(batch.size());
            inserted = true;
            retries = 0;
          } catch (MongoException e) {
            logger.error("Thread {}: Error inserting batch: {}", threadId, e.getMessage());
            metricsManager.incrementFailedOperations();
            retries++;
            if (retries < MAX_RETRIES) {
              logger.warn(
                  "Thread {}: Retrying in {} ms (Attempt {} of {})",
                  threadId,
                  RETRY_DELAY_MS,
                  retries,
                  MAX_RETRIES);
              try {
                Thread.sleep(RETRY_DELAY_MS);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.error("Thread {}: Interrupted during retry delay", threadId);
                return;
              }
            } else {
              logger.error("Thread {}: Max retries reached. Skipping batch.", threadId);
            }
          }
        }
        batch.clear();
      }
    }
  }
}
