package com.example;

public class Config {
  private final String connectionString;
  private final String databaseName;
  private final String collectionName;
  private final double totalDataSizeGB;
  private final int writePercentage;
  private final int numThreads;
  private final int documentsPerThread;
  private final int targetDocumentSize;

  private Config(
      String connectionString,
      String databaseName,
      String collectionName,
      double totalDataSizeGB,
      int writePercentage,
      int numThreads,
      int targetDocumentSize) {
    this.connectionString = connectionString;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.totalDataSizeGB = totalDataSizeGB;
    this.writePercentage = writePercentage;
    this.numThreads = numThreads;
    this.targetDocumentSize = targetDocumentSize;
    long totalDocuments = (long) (totalDataSizeGB * 1024 * 1024 * 1024) / targetDocumentSize;
    this.documentsPerThread = (int) (totalDocuments / numThreads);
  }

  public static Config fromEnv() {
    return new Config(
        System.getenv("MONGODB_URI"),
        System.getenv().getOrDefault("MONGODB_DATABASE", "test"),
        System.getenv().getOrDefault("MONGODB_COLLECTION", "java"),
        Double.parseDouble(System.getenv().getOrDefault("TOTAL_DATA_SIZE_GB", "1")),
        Integer.parseInt(System.getenv().getOrDefault("WRITE_PERCENTAGE", "5")),
        Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "8")),
        Integer.parseInt(System.getenv().getOrDefault("TARGET_DOCUMENT_SIZE", "4096")));
  }

  // Getters for all fields
  public String getConnectionString() {
    return connectionString;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public double getTotalDataSizeGB() {
    return totalDataSizeGB;
  }

  public int getWritePercentage() {
    return writePercentage;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public int getDocumentsPerThread() {
    return documentsPerThread;
  }

  public int getTargetDocumentSize() {
    return targetDocumentSize;
  }
}
