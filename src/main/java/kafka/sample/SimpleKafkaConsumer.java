package kafka.sample;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

@Slf4j
public class SimpleKafkaConsumer {

  private static final String sasl_username = "humble-gorilla.4DNBS9UESC6EYJG"; // prod

  // nonprod
  // private static final String sasl_password =
  //     "iQBtqUmhAJ1le_OVr_lXVs7opHXWerDhCxkefYTQLXVzY9r5i1ULSkepWC-r8Lj1-ST-Ergm7YfIKNck8Eg62wC7Y5Q6GqqOi7zk88G-K8-ItgZ6lPYjkDRlNstweYXx";

  // prod
  private static final String sasl_password =
      "MrMhgRcyvQO81p4w7s-0xT5LbupFpb6q4sVS7M21QV8txH46mFAr6p14NqWUDbhQOHHH1qO2DXPXuUt8VDfwH4HI1DHjATZDczdoD6n0PxoksFLqzU1qHM84__J9VF2K";


  private static final String kafka_broker_prod = "kafka.prod.us-west-2.aws.proton.nordstrom.com:9093";
  private static final String kafka_broker_nonprod = "kafka.nonprod.us-west-2.aws.proton.nordstrom.com:9093";


  private static final String schema_registry_url_prod = "https://schema-registry.prod.us-west-2.aws.proton.nordstrom.com";
  private static final String schema_registry_url_nonprod = "https://schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com";

  public static void main(String[] args) {
    //
    //
    // log.info("[Test={}]", "data");
    //
    // log.error("TEST ERROR");
    fetchFromPublicClusterSpecificRecord();
    // fetchFromLocalCluster();
  }

  private static void fetchFromPublicClusterSpecificRecord() {
    String topicName = "wine-events-avro";

    Properties props = new Properties();

    var jaasTemplate =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    var jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

    props.put("bootstrap.servers", kafka_broker_prod);
    props.put("group.id", "sample-consumer-1");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("auto.offset.reset", "latest");
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "SCRAM-SHA-512");
    props.put("sasl.jaas.config", jaasConfig);

    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_registry_url_prod);

    props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");

    KafkaConsumer<SpecificRecord, SpecificRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topicName));

    //    consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
    log.debug("Starting fetching messages from " + topicName);
    try {
      while (true) {
        ConsumerRecords<SpecificRecord, SpecificRecord> records =
            consumer.poll(Duration.ofMillis(100));

        for (var record : records) {
          System.out.println(record.value().toString());
        }
      }
    } finally {
      consumer.close();
    }
  }

  private static void fetchFromPublicClusterGenericRecord() {
    String topicName = "WINE_SourceEvents_avro";

    Properties props = new Properties();

    var jaasTemplate =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    var jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

    props.put("bootstrap.servers", "kafka.nonprod.us-west-2.aws.proton.nordstrom.com:9093");
    props.put("group.id", "WINE_RawEvents_Processor_staging");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", KafkaAvroDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put("auto.offset.reset", "latest");
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "SCRAM-SHA-512");
    props.put("sasl.jaas.config", jaasConfig);

    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    props.put(
        "schema.registry.url",
        "https://schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com");

    props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");

    //    props.put(
    //        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
    //        LogAndContinueExceptionHandler.class);

    // props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
    //     "https://schema-registry.nonprod.us-west-2.aws.proton.nordstrom.com");

    props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);

    KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topicName));

    //    consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
    log.debug("Starting fetching messages from " + topicName);
    try {
      while (true) {
        ConsumerRecords<GenericRecord, GenericRecord> records =
            consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord record : records) {
          System.out.println(
              "Consumer record: "
                  + ((GenericRecord) record.key()).get("facilityId")
                  + " "
                  + record.value().toString());
        }
      }
    } finally {
      consumer.close();
    }
  }

  private static void fetchFromLocalCluster() {
    String topicName = "test_avro";

    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("application.id", "local-consumer-1");
    props.put("group.id", "local-consumer-staging");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("auto.offset.reset", "earliest");

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    // props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
    //     LogAndContinueExceptionHandler.class);

    props.put("schema.registry.url", "http://localhost:8081");

    KafkaConsumer<SpecificRecord, SpecificRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topicName));

    //    consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
    log.debug("Starting fetching messages from " + topicName);
    try {
      while (true) {
        ConsumerRecords<SpecificRecord, SpecificRecord> records =
            consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<SpecificRecord, SpecificRecord> record : records) {

          System.out.println(
              String.format(
                  "Consumer record: (%s, %s, %s) ",
                  record.key(), record.value().get(6), record.partition()));

          consumer.commitSync();
        }
      }
    } finally {
      consumer.close();
    }
  }
}
