package com.example;

public class Config {
  private final String connectionString;
  private final String databaseName;
  private final String collectionName;
  private final double totalDataSizeGB;
  private final int writePercentage;
  private final int numThreads;
  private final int documentsPerThread;

  private Config(
      String connectionString,
      String databaseName,
      String collectionName,
      double totalDataSizeGB,
      int writePercentage,
      int numThreads) {
    this.connectionString = connectionString;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.totalDataSizeGB = totalDataSizeGB;
    this.writePercentage = writePercentage;
    this.numThreads = numThreads;
    long totalDocuments =
        (long) (totalDataSizeGB * 1024 * 1024 * 1024) / MongoDBScalingTest.TARGET_DOCUMENT_SIZE;
    this.documentsPerThread = (int) (totalDocuments / numThreads);
  }

  public static Config fromEnv() {
    return new Config(
        System.getenv("MONGODB_URI"),
        System.getenv("MONGODB_DATABASE"),
        System.getenv("MONGODB_COLLECTION"),
        Double.parseDouble(System.getenv("TOTAL_DATA_SIZE_GB")),
        Integer.parseInt(System.getenv("WRITE_PERCENTAGE")),
        Integer.parseInt(System.getenv("NUM_THREADS")));
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
}
