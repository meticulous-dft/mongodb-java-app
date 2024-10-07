package com.example;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import net.datafaker.Faker;
import net.datafaker.providers.base.Name;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class DocumentGenerator {
  private static final Faker FAKER = new Faker(new Locale("en-US"));

  public static Document generateRichDocument(int index) {
    Document doc =
        new Document("_id", UUID.randomUUID().toString())
            .append("index", index)
            .append("timestamp", Instant.now().toEpochMilli())
            .append("user", generateUser())
            .append("order", generateOrder())
            .append("metadata", generateMetadata());

    // Calculate document size
    int currentSize = calculateSize(doc);
    if (currentSize < MongoDBScalingTest.TARGET_DOCUMENT_SIZE) {
      int paddingSize =
          MongoDBScalingTest.TARGET_DOCUMENT_SIZE - currentSize - 10; // 10 bytes buffer
      doc.append("padding", FAKER.lorem().characters(Math.max(0, paddingSize)));
    }

    return doc;
  }

  public static int calculateSize(Document doc) {
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      new BsonDocumentCodec()
          .encode(writer, doc.toBsonDocument(), EncoderContext.builder().build());
    }
    return buffer.getSize();
  }

  private static Document generateUser() {
    Name name = FAKER.name();
    return new Document("firstName", name.firstName())
        .append("lastName", name.lastName())
        .append("age", FAKER.number().numberBetween(18, 90))
        .append("email", FAKER.internet().emailAddress())
        .append("address", generateAddress())
        .append("profile", generateProfile());
  }

  private static Document generateAddress() {
    return new Document("street", FAKER.address().streetAddress())
        .append("city", FAKER.address().city())
        .append("country", FAKER.address().country())
        .append("postalCode", FAKER.address().zipCode());
  }

  private static Document generateProfile() {
    return new Document("occupation", FAKER.job().title())
        .append("company", FAKER.company().name())
        .append("education", FAKER.educator().course())
        .append("interests", FAKER.collection(() -> FAKER.hobby().activity()).len(3, 5).generate());
  }

  private static Document generateOrder() {
    List<Document> items =
        IntStream.range(0, FAKER.number().numberBetween(1, 6))
            .mapToObj(
                i ->
                    new Document("product", FAKER.commerce().productName())
                        .append("category", FAKER.commerce().department())
                        .append("quantity", FAKER.number().numberBetween(1, 10))
                        .append("price", FAKER.number().randomDouble(2, 1, 1000)))
            .toList();

    return new Document("items", items)
        .append(
            "total",
            items.stream()
                .mapToDouble(item -> item.getInteger("quantity") * item.getDouble("price"))
                .sum())
        .append("status", FAKER.options().option("pending", "processing", "shipped", "delivered"))
        .append("orderDate", FAKER.timeAndDate().past(30, TimeUnit.DAYS).toEpochMilli())
        .append("shippingMethod", FAKER.options().option("Standard", "Express", "Next Day"))
        .append("paymentMethod", FAKER.finance().creditCard());
  }

  private static Document generateMetadata() {
    return new Document("userAgent", FAKER.internet().userAgent())
        .append("ipAddress", FAKER.internet().ipV4Address())
        .append(
            "location",
            new Document("latitude", FAKER.address().latitude())
                .append("longitude", FAKER.address().longitude()))
        .append("lastLogin", FAKER.timeAndDate().past(7, TimeUnit.DAYS).toEpochMilli())
        .append("deviceType", FAKER.options().option("desktop", "mobile", "tablet"))
        .append("browser", FAKER.internet().userAgent())
        .append("operatingSystem", FAKER.computer().operatingSystem());
  }
}
