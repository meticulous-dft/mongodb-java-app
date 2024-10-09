# Use the official maven image to create a build artifact.
FROM maven:3.8.4-openjdk-17-slim AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package

# Use OpenJDK for the runtime image
FROM openjdk:17-slim
WORKDIR /app
COPY --from=build /app/target/mongodb-scaling-test-1.0-SNAPSHOT.jar ./mongodb-scaling-test.jar

# Create a non-root user to run the application
RUN useradd -m myuser
USER myuser

ENTRYPOINT ["java", "-jar", "mongodb-scaling-test.jar"]
# The default command runs the load test, override with "load" to run data loading
CMD ["test"]