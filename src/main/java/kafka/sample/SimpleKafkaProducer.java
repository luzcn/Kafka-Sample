package kafka.sample;

import com.nordstrom.canonical.Event;
import com.nordstrom.canonical.InventoryAdjustment;
import com.nordstrom.canonical.InventoryNode;
import com.nordstrom.canonical.Key;
import com.nordstrom.canonical.Location;
import com.nordstrom.canonical.Sku;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SimpleKafkaProducer {

  // private static final String topicName = "WINE_ProcessedEvents_avro";
  // https://kafka-manager.proton.platform.r53.nordstrom.net/clusters/nonprod/topics/WINE_TestEvents_string

  private static final String sasl_username = "humble-gorilla.4X0BQT84X4EU5W5";
  private static final String sasl_password =
      "iQBtqUmhAJ1le_OVr_lXVs7opHXWerDhCxkefYTQLXVzY9r5i1ULSkepWC-r8Lj1-ST-Ergm7YfIKNck8Eg62wC7Y5Q6GqqOi7zk88G-K8-ItgZ6lPYjkDRlNstweYXx";

  private static final Event event =
      Event.newBuilder()
          .setVersion("1.0")
          .setId("abc123345677789")
          .setCorrelationId("c226a9a2ff4b601e")
          .setCreatedAt("2018-06-22T10:30:06.000Z")
          .setPublishedAt("2018-06-22T10:30:06.000Z")
          .setEntityId("null")
          .setEventName("SkuAdjusted")
          .setSources(List.of("WMi"))
          .setFacilityId("123")
          .setAdjustments(
              List.of(
                  InventoryAdjustment.newBuilder()
                      .setBelongsTo("Belongs To")
                      .setWhy("reason code")
                      .setChannel("FullPrice")
                      .setLocation(
                          Location.newBuilder().setFacilityId("89").setLogicalId("399").build())
                      .setInventory(
                          InventoryNode.newBuilder()
                              .setQuantity(Map.of("WM", 1005))
                              .setDelta(Map.of("WM", 5))
                              .setType("category")
                              .setChildren(
                                  Map.of(
                                      "stockOnHand",
                                      InventoryNode.newBuilder()
                                          .setType("category")
                                          .setQuantity(Map.of("WM", 1005))
                                          .setDelta(Map.of("WM", 5))
                                          .setChildren(
                                              Map.of(
                                                  "sellable",
                                                  InventoryNode.newBuilder()
                                                      .setType("category")
                                                      .setQuantity(Map.of("WM", 1005))
                                                      .setDelta(Map.of("WM", 5))
                                                      .setChildren(Collections.emptyMap())
                                                      .build()))
                                          .build()))
                              .build())
                      .setSku(
                          Sku.newBuilder()
                              .setChannel("FullPrice")
                              .setLegacyNumber("97099496")
                              .setNumber("2021625864")
                              .setChannel("FullPrice")
                              .build())
                      .build()))
          .build();

  public static void main(String[] args) {

    try {

      // createTopic("test-avro", 15);

      sendToLocalAvro();
      // sendToPublicClusterAvro();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private static void sendToLocalAvro() throws IOException {
    Properties props = new Properties();

    String topicName = "test_avro";

    props.put("bootstrap.servers", "localhost:9092");
    props.put("application.id", "kafka-producer-sample");

    // Specify the criteria of which requests are considered complete, "all" is the slowest but most
    // durable.
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // When write to topic request failed, there is no retry
    props.put(ProducerConfig.RETRIES_CONFIG, "0");

    // The buffer size of unsent records
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16348);

    // Producer will wait for more records to group in one batch, before sending requests
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

    props.put("schema.registry.url", "http://localhost:8081");

    KafkaProducer<Key, Event> producer = new KafkaProducer<>(props);

    try {

      ProducerRecord<Key, Event> record =
          new ProducerRecord<>(
              topicName, Key.newBuilder().setFacilityId(event.getFacilityId()).build(), event);
      var metadata = producer.send(record);

      // System.out.println("Sent message " + metadata);
      log.info("Send Message: {}, {}", metadata.get().offset(), metadata.get().partition());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }

  private static void sendToPublicClusterAvro() throws IOException {
    Properties props = new Properties();

    String topicName = "warehouse-events-avro";

    var jaasTemplate =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    var jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

    props.put("bootstrap.servers", "kafka.nonprod.us-west-2.aws.proton.nordstrom.com:9093");

    props.put("application.id", "proton-producer-sample");

    // Specify the criteria of which requests are considered complete, "all" is the slowest but most
    // durable.
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // When write to topic request failed, there is no retry
    props.put(ProducerConfig.RETRIES_CONFIG, "0");

    // The buffer size of unsent records
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16348);

    // Producer will wait for more records to group in one batch, before sending requests
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);

    // props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

    //    props.put(KafkaAvroSerializerConfig.S)
    //    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "SCRAM-SHA-512");
    props.put("sasl.jaas.config", jaasConfig);
    //
    // String schema_registry_url = "https://" + sasl_username + ":" + sasl_password
    //     + "@schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com";

    props.put(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "https://schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com");

    //    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
    //        "https://" + sasl_username + ":" + sasl_password
    //            + "@schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com");

    props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");

    // props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
    //     "https://schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com");
    //    props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);

    KafkaProducer<SpecificRecord, SpecificRecord> producer = new KafkaProducer<>(props);
    try {

      Key key = new Key(event.getFacilityId());
      //      ProducerRecord<SpecificRecord, SpecificRecord> record = new
      // ProducerRecord<>(topicName,
      //          key,
      //          event);

      for (int i = 0; i < 10; i++) {
        var metadata = producer.send(new ProducerRecord<>(topicName, key, event));

        System.out.println(metadata.get());
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }


  private static void createTopic(final String topicName,
      final int partitions) {

    final short replicationFactor = 1;

    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("application.id", "kafka-admin-client");

    // Specify the criteria of which requests are considered complete, "all" is the slowest but most
    // durable.
    // props.put(ProducerConfig.ACKS_CONFIG, "all");
    //
    // // When write to topic request failed, there is no retry
    // props.put(ProducerConfig.RETRIES_CONFIG, "0");
    //
    // // The buffer size of unsent records
    // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16348);
    //
    // // Producer will wait for more records to group in one batch, before sending requests
    // props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    //
    // props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    // Create admin client
    try (final AdminClient adminClient = KafkaAdminClient.create(props)) {
      try {
        // Define topic
        final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        // Create topic, which is async call.
        final CreateTopicsResult createTopicsResult = adminClient
            .createTopics(Collections.singleton(newTopic));

        // Since the call is Async, Lets wait for it to complete.
        createTopicsResult.values().get(topicName).get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
