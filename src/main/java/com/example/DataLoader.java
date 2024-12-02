package com.example;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
  private final AtomicLong insertedDocuments;
  private final long totalDocuments;
  private final int targetDocumentSize;
  private static final AtomicBoolean indexCreated = new AtomicBoolean(false);
  private final int threadId;

  public DataLoader(
      MongoCollection<Document> collection,
      int documentsToLoad,
      int startIndex,
      AtomicLong insertedDocuments,
      long totalDocuments,
      int targetDocumentSize,
      int threadId) {
    this.collection = collection;
    this.documentsToLoad = documentsToLoad;
    this.startIndex = startIndex;
    this.insertedDocuments = insertedDocuments;
    this.totalDocuments = totalDocuments;
    this.targetDocumentSize = targetDocumentSize;
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
            collection.insertMany(batch);
            long count = insertedDocuments.addAndGet(batch.size());
            logger.debug(
                "Thread {}: {} documents loaded. Total: {} / {}",
                threadId,
                batch.size(),
                count,
                totalDocuments);
            inserted = true;
            retries = 0;
          } catch (MongoException e) {
            logger.error("Thread {}: Error inserting batch: {}", threadId, e.getMessage());
            retries++;
            if (retries < MAX_RETRIES) {
              logger.info(
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
    logger.debug("Thread {}: Finished loading {} documents", threadId, documentsToLoad);
  }
}
