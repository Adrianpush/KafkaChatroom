package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class User {

    String name;
//    Queue<String> cache = new LinkedList<>();
    Producer<String, String> producer;
    Consumer<String, String> consumer;

    public User(String name) {
        this.name = name;
        createProducer();
        createConsumer();
    }

    public void createChatroom(String chatroomName) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectionDetails.BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(chatroomName, 6, (short) 1);
            newTopic.configs(Collections.singletonMap("retention.ms", "86400000"));
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic " + chatroomName + " created successfully.");
        } catch (InterruptedException | ExecutionException e) {
            throw new ChatroomCreateException("Unable to crate chatroom", e);
        }
    }

    public void sendMessage(String chatroom, String message) {
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(chatroom, String.valueOf(message.length()), message);
        producer.send(kafkaRecord);
    }

    public void readNewMessages(String chatroom) {
        consumer.subscribe(Collections.singletonList(chatroom));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(kafkaRecord -> System.out.println("Received message: " + kafkaRecord.value()));
    }

    public void logOut() {
        this.close();
    }

    private void createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaConnectionDetails.BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectionDetails.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, name);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    private void close() {
        producer.close();
        consumer.close();
    }
}
