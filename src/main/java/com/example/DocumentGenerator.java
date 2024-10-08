package com.example;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.datafaker.Faker;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class DocumentGenerator {
  private static final Faker FAKER = new Faker(new Locale("en-US"));

  public static Document generateRichDocument(int index, int targetSize) {
    Document doc =
        new Document()
            .append("index", index)
            .append("timestamp", Instant.now().toEpochMilli())
            .append("user", generateUser())
            .append("order", generateOrder())
            .append("product", generateProduct())
            .append("shipping", generateShipping())
            .append("payment", generatePayment())
            .append("metadata", generateMetadata())
            .append("tags", generateTags())
            .append("comments", generateComments());

    // Calculate document size
    int currentSize = calculateSize(doc);
    if (currentSize < targetSize) {
      int paddingSize = targetSize - currentSize - 10; // 10 bytes buffer
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
    return new Document("firstName", FAKER.name().firstName())
        .append("lastName", FAKER.name().lastName())
        .append("email", FAKER.internet().emailAddress())
        .append("phone", FAKER.phoneNumber().phoneNumber())
        .append("address", generateAddress())
        .append("dateOfBirth", FAKER.date().birthday().toInstant().toEpochMilli())
        .append(
            "registrationDate",
            Instant.now().minusSeconds(FAKER.number().numberBetween(0L, 31536000L)).toEpochMilli())
        .append(
            "lastLoginDate",
            Instant.now().minusSeconds(FAKER.number().numberBetween(0L, 2592000L)).toEpochMilli())
        .append("preferences", generatePreferences());
  }

  private static Document generateAddress() {
    return new Document("street", FAKER.address().streetAddress())
        .append("city", FAKER.address().city())
        .append("state", FAKER.address().state())
        .append("country", FAKER.address().country())
        .append("zipCode", FAKER.address().zipCode())
        .append(
            "coordinates",
            new Document("lat", FAKER.address().latitude())
                .append("lng", FAKER.address().longitude()));
  }

  private static Document generatePreferences() {
    return new Document("language", FAKER.nation().language())
        .append("currency", FAKER.currency().code())
        .append("timezone", FAKER.address().timeZone())
        .append("marketingEmails", FAKER.bool().bool())
        .append("twoFactorAuth", FAKER.bool().bool());
  }

  private static Document generateOrder() {
    List<Document> items =
        IntStream.range(0, FAKER.number().numberBetween(1, 6))
            .mapToObj(
                i ->
                    new Document("productId", UUID.randomUUID().toString())
                        .append("name", FAKER.commerce().productName())
                        .append("quantity", FAKER.number().numberBetween(1, 10))
                        .append("price", FAKER.number().randomDouble(2, 1, 1000))
                        .append("discount", FAKER.number().randomDouble(2, 0, 1)))
            .collect(Collectors.toList());

    double subtotal =
        items.stream()
            .mapToDouble(item -> item.getInteger("quantity") * item.getDouble("price"))
            .sum();
    double discount =
        items.stream()
            .mapToDouble(
                item ->
                    item.getInteger("quantity")
                        * item.getDouble("price")
                        * item.getDouble("discount"))
            .sum();
    double tax = (subtotal - discount) * 0.1; // Assuming 10% tax
    double total = subtotal - discount + tax;

    return new Document("orderId", UUID.randomUUID().toString())
        .append(
            "orderDate",
            Instant.now().minusSeconds(FAKER.number().numberBetween(0L, 2592000L)).toEpochMilli())
        .append("items", items)
        .append("subtotal", subtotal)
        .append("discount", discount)
        .append("tax", tax)
        .append("total", total)
        .append(
            "status",
            FAKER.options().option("pending", "processing", "shipped", "delivered", "cancelled"));
  }

  private static Document generateProduct() {
    return new Document("productId", UUID.randomUUID().toString())
        .append("name", FAKER.commerce().productName())
        .append("description", FAKER.lorem().paragraph())
        .append("category", FAKER.commerce().department())
        .append("subcategory", FAKER.commerce().material())
        .append("price", FAKER.number().randomDouble(2, 1, 1000))
        .append("currency", FAKER.currency().code())
        .append("inStock", FAKER.number().numberBetween(0, 1000))
        .append(
            "attributes",
            new Document("color", FAKER.color().name())
                .append("size", FAKER.options().option("XS", "S", "M", "L", "XL", "XXL"))
                .append("weight", FAKER.number().randomDouble(2, 1, 100))
                .append(
                    "dimensions",
                    new Document("length", FAKER.number().randomDouble(2, 1, 100))
                        .append("width", FAKER.number().randomDouble(2, 1, 100))
                        .append("height", FAKER.number().randomDouble(2, 1, 100))));
  }

  private static Document generateShipping() {
    return new Document(
            "method", FAKER.options().option("Standard", "Express", "Next Day", "International"))
        .append("carrier", FAKER.options().option("UPS", "FedEx", "DHL", "USPS"))
        .append("trackingNumber", FAKER.expression("#{numerify '############'}"))
        .append(
            "estimatedDelivery",
            Instant.now()
                .plusSeconds(FAKER.number().numberBetween(86400L, 1209600L))
                .toEpochMilli()) // 1-14 days
        .append("shippingAddress", generateAddress());
  }

  private static Document generatePayment() {
    return new Document(
            "method",
            FAKER.options().option("Credit Card", "PayPal", "Bank Transfer", "Cash on Delivery"))
        .append("transactionId", UUID.randomUUID().toString())
        .append("amount", FAKER.number().randomDouble(2, 10, 1000))
        .append("currency", FAKER.currency().code())
        .append("status", FAKER.options().option("pending", "completed", "failed", "refunded"))
        .append(
            "cardType",
            FAKER.options().option("Visa", "MasterCard", "American Express", "Discover"))
        .append("last4", FAKER.expression("#{numerify '####'}"));
  }

  private static Document generateMetadata() {
    return new Document("userAgent", FAKER.internet().userAgent())
        .append("ipAddress", FAKER.internet().ipV4Address())
        .append("referrer", FAKER.internet().url())
        .append("sessionId", UUID.randomUUID().toString());
  }

  private static List<String> generateTags() {
    return IntStream.range(0, FAKER.number().numberBetween(1, 6))
        .mapToObj(i -> FAKER.lorem().word())
        .collect(Collectors.toList());
  }

  private static List<Document> generateComments() {
    return IntStream.range(0, FAKER.number().numberBetween(0, 5))
        .mapToObj(
            i ->
                new Document("userId", UUID.randomUUID().toString())
                    .append("username", FAKER.internet().username())
                    .append("comment", FAKER.lorem().sentence())
                    .append("rating", FAKER.number().numberBetween(1, 6))
                    .append(
                        "timestamp",
                        Instant.now()
                            .minusSeconds(FAKER.number().numberBetween(0L, 2592000L))
                            .toEpochMilli()))
        .collect(Collectors.toList());
  }
}
