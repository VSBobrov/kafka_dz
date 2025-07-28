package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import example.Purchase;
import example.Product;

public class PurchaseAlertApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchase-alerts-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // AVRO Serdes setup
        final SpecificAvroSerde<Purchase> purchaseSerde = new SpecificAvroSerde<>();
        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfig = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"
        );
        purchaseSerde.configure(serdeConfig, false);
        productSerde.configure(serdeConfig, false);

        // Building topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read product table
        KTable<String, Product> productTable = builder.table(
            "product-topic",
            Consumed.with(Serdes.String(), productSerde)
        );

        // Read purchase stream
        KStream<String, Purchase> purchaseStream = builder.stream(
            "purchase-topic",
            Consumed.with(Serdes.String(), purchaseSerde)
        );

        // Join purchases with products
        KStream<String, Double> joined = purchaseStream.join(
            productTable,
            (purchase, product) -> {
                if (product == null) return 0.0;
                return product.getPrice() * purchase.getQuantity();
            }
        );

        // Aggregate and alert if total > 3000 in 1 minute
        joined
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .reduce(Double::sum)
            .toStream()
            .filter((windowedKey, sum) -> sum > 3000)
            .map((windowedKey, sum) -> KeyValue.pair(windowedKey.key(), "ALERT: Total = " + sum))
            .to("alert-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Start streaming app
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
