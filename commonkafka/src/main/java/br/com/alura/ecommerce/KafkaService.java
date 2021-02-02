package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<Order> type, Map<String, String> properties){
        this(parse, groupId, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }
    KafkaService(String groupId, Pattern topic, ConsumerFunction parse,  Class<Order> type, Map<String, String> properties){
        this(parse, groupId, type, properties);
        consumer.subscribe(topic);
    }

    public KafkaService(ConsumerFunction parse, String groupId,  Class<Order> type, Map<String, String> properties){
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
    }


    public void run(){
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Found " + records.count() + " records");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        // only catches Exception because no matter which Exception. I wan to recover and parse the next one
                        // so far, just logging the exception for this message
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties getProperties( Class<Order> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserialize.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserialize.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}