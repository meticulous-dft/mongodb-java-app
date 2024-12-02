package com.example;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {
  private static final Logger logger = LoggerFactory.getLogger(MetricsManager.class);
  private static final MetricsManager INSTANCE = new MetricsManager();
  private final OpenTelemetry openTelemetry;
  private final Meter meter;
  private final LongCounter totalOperations;
  private final LongCounter readOperations;
  private final LongCounter writeOperations;
  private final LongCounter failedOperations;
  private final AtomicReference<Double> latestReadLatency = new AtomicReference<>(0.0);
  private final AtomicReference<Double> latestWriteLatency = new AtomicReference<>(0.0);
  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final AtomicLong totalOperationsCount = new AtomicLong(0);
  private final AtomicLong readOperationsCount = new AtomicLong(0);
  private final AtomicLong writeOperationsCount = new AtomicLong(0);
  private final AtomicLong failedOperationsCount = new AtomicLong(0);

  private MetricsManager() {
    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.of(ResourceAttributes.SERVICE_NAME, "mongodb-java-app")));

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(LoggingMetricExporter.create()).build())
            .build();

    openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();

    meter = openTelemetry.getMeter("com.example.mongodb-java-app");

    totalOperations =
        meter
            .counterBuilder("total_operations")
            .setDescription("Total number of operations")
            .build();

    readOperations =
        meter.counterBuilder("read_operations").setDescription("Number of read operations").build();

    writeOperations =
        meter
            .counterBuilder("write_operations")
            .setDescription("Number of write operations")
            .build();

    failedOperations =
        meter
            .counterBuilder("failed_operations")
            .setDescription("Number of failed operations")
            .build();

    ObservableDoubleGauge readLatency =
        meter
            .gaugeBuilder("read_latency")
            .setDescription("Read operation latency")
            .setUnit("ms")
            .buildWithCallback(measurement -> measurement.record(latestReadLatency.get()));

    ObservableDoubleGauge writeLatency =
        meter
            .gaugeBuilder("write_latency")
            .setDescription("Write operation latency")
            .setUnit("ms")
            .buildWithCallback(measurement -> measurement.record(latestWriteLatency.get()));

    ObservableDoubleGauge throughput =
        meter
            .gaugeBuilder("throughput")
            .setDescription("Operations per second")
            .setUnit("ops/s")
            .buildWithCallback(
                measurement -> {
                  long elapsedSeconds = (System.currentTimeMillis() - startTime.get()) / 1000;
                  if (elapsedSeconds > 0) {
                    measurement.record((double) totalOperationsCount.get() / elapsedSeconds);
                  }
                });
  }

  public static MetricsManager getInstance() {
    return INSTANCE;
  }

  public void printCurrentMetrics() {
    long currentTime = System.currentTimeMillis();
    long elapsedSeconds = (currentTime - startTime.get()) / 1000;
    long totalOps = totalOperationsCount.get();
    double throughput = elapsedSeconds > 0 ? (double) totalOps / elapsedSeconds : 0;

    logger.info("--- Current Metrics ---");
    logger.info("Elapsed Time: {} seconds", elapsedSeconds);
    logger.info("Total Operations: {}", totalOps);
    logger.info("Read Operations: {}", readOperationsCount.get());
    logger.info("Write Operations: {}", writeOperationsCount.get());
    logger.info("Failed Operations: {}", failedOperationsCount.get());
    logger.info("Throughput: {} ops/sec", String.format("%.2f", throughput));
    logger.info("Latest Read Latency: {} ms", String.format("%.2f", latestReadLatency.get()));
    logger.info("Latest Write Latency: {} ms", String.format("%.2f", latestWriteLatency.get()));

    // Add cluster state information
    ClusterState clusterState = ClusterState.getInstance();
    logger.info("Cluster State: {}", clusterState);
  }

  public void incrementTotalOperations() {
    totalOperations.add(1);
    totalOperationsCount.incrementAndGet();
  }

  public void incrementReadOperations() {
    readOperations.add(1);
    readOperationsCount.incrementAndGet();
  }

  public void incrementWriteOperations() {
    writeOperations.add(1);
    writeOperationsCount.incrementAndGet();
  }

  public void incrementFailedOperations() {
    failedOperations.add(1);
    failedOperationsCount.incrementAndGet();
  }

  public void recordReadLatency(double latencyMs) {
    latestReadLatency.set(latencyMs);
  }

  public void recordWriteLatency(double latencyMs) {
    latestWriteLatency.set(latencyMs);
  }

  public void resetStartTime() {
    startTime.set(System.currentTimeMillis());
    totalOperationsCount.set(0);
    readOperationsCount.set(0);
    writeOperationsCount.set(0);
    failedOperationsCount.set(0);
  }

  public AtomicLong getTotalOperationsCount() {
    return totalOperationsCount;
  }
}
