java_binary(
    name = "mongodb-java-perf",
    srcs = glob(["src/main/java/com/example/*.java"]),
    main_class = "com.example.Main",
    resources = ["src/main/resources/logback.xml"],
    deps = [
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:io_opentelemetry_opentelemetry_api",
        "@maven//:io_opentelemetry_opentelemetry_exporter_logging",
        "@maven//:io_opentelemetry_opentelemetry_sdk",
        "@maven//:io_opentelemetry_opentelemetry_sdk_common",
        "@maven//:io_opentelemetry_opentelemetry_sdk_metrics",
        "@maven//:io_opentelemetry_opentelemetry_semconv",
        "@maven//:net_datafaker_datafaker",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
