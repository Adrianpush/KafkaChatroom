package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.chatroom.KafkaConnectionDetails;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class User {

    String name;
    Producer<String, String> producer;
    Consumer<String, String> consumer;
    String currentChatRoom;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public User(String name) {
        this.name = name;
        createProducer();
        createConsumer();
    }

    public String getCurrentChatRoom() {
        return currentChatRoom;
    }

    public void joinChatRoom(String chatRoom) {
        try {
            currentChatRoom = chatRoom;
            consumer.unsubscribe();
            consumer.subscribe(Collections.singletonList(currentChatRoom));
            executorService.awaitTermination(10, TimeUnit.SECONDS);
            executorService.submit(() -> readNewMessages(currentChatRoom));
            System.out.println("Joined " + chatRoom);
        } catch (RuntimeException | InterruptedException e) {
            System.out.println("Cannot subscribe to chatroom: " + e.getMessage());
        }

    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                currentChatRoom,
                String.valueOf(currentChatRoom.hashCode()), name + ": " + message);
        producer.send(kafkaRecord);
        System.out.println("sent " + message);
    }

    private void readNewMessages(String chatRoom) {
        for(TopicPartition topicPartition: consumer.assignment()) {
            System.out.println(consumer.position(topicPartition));
            consumer.seek(topicPartition, 0);
        }
        while (chatRoom.equalsIgnoreCase(currentChatRoom)) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(kafkaRecord ->
                    System.out.println(kafkaRecord.topic() + ": " + kafkaRecord.value()));
            consumer.commitSync();
        }
        System.out.println("reading loop closed.");
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    private void close() {
        producer.close();
        consumer.close();
    }
}
