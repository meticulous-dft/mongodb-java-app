package com.example;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger("com.example.DataLoad");
  private final MongoCollection<Document> collection;
  private final int documentsToLoad;
  private final int startIndex;
  private final AtomicLong insertedDocuments;
  private final long totalDocuments;
  private final int targetDocumentSize;
  private static final AtomicBoolean indexCreated = new AtomicBoolean(false);

  public DataLoader(
      MongoCollection<Document> collection,
      int documentsToLoad,
      int startIndex,
      AtomicLong insertedDocuments,
      long totalDocuments,
      int targetDocumentSize) {
    this.collection = collection;
    this.documentsToLoad = documentsToLoad;
    this.startIndex = startIndex;
    this.insertedDocuments = insertedDocuments;
    this.totalDocuments = totalDocuments;
    this.targetDocumentSize = targetDocumentSize;
  }

  @Override
  public void run() {
    createIndexIfNeeded();
    loadDocuments();
  }

  public void createIndexIfNeeded() {
    if (indexCreated.compareAndSet(false, true)) {
      logger.info("Creating index on 'index' field");
      collection.createIndex(Indexes.ascending("index"), new IndexOptions().background(true));
      logger.info("Index creation completed");
    }
  }

  private void loadDocuments() {
    List<Document> batch = new ArrayList<>();
    for (int i = 0; i < documentsToLoad; i++) {
      batch.add(DocumentGenerator.generateRichDocument(startIndex + i, targetDocumentSize));

      if (batch.size() == 1000 || i == documentsToLoad - 1) {
        collection.insertMany(batch);
        long inserted = insertedDocuments.addAndGet(batch.size());
        logger.debug("{} documents loaded. Total: {} / {}", batch.size(), inserted, totalDocuments);
        batch.clear();
      }
    }
  }
}
