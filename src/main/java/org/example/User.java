package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.chatroom.KafkaConnectionDetails;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class User {

    String name;
    Producer<String, String> producer;
    Consumer<String, String> consumer;
    TopicPartition topicPartition;
    String currentChatRoom;
    ExecutorService printService;

    public User(String name) {
        this.name = name;
        this.printService = Executors.newSingleThreadExecutor();
        createProducer();
        createConsumer();
    }

    public void joinChatRoom(String chatRoom, boolean fromStart) {
        try {
            topicPartition = new TopicPartition(chatRoom, 0);
            currentChatRoom = chatRoom;
            consumer.assign(List.of(topicPartition));
            if (fromStart) resetOffset();
            printService.submit(this::readNewMessages);
            System.out.println("Joined " + chatRoom);
        } catch (RuntimeException e) {
            System.out.println("Cannot subscribe to chatroom: " + e.getMessage());
        }
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(currentChatRoom, name, message);
        producer.send(kafkaRecord);
    }

    private void readNewMessages() {
        while (currentChatRoom != null) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(kafkaRecord ->
                    System.out.println(kafkaRecord.topic() + " - " + kafkaRecord.key() + ": " + kafkaRecord.value()));
            consumer.commitSync();
        }
    }

    private void resetOffset() {
        consumer.seek(topicPartition, 0);
        consumer.commitSync();
    }

    public void leaveChatRoom() {
        currentChatRoom = null;
        System.out.println("Left chatroom");
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
        leaveChatRoom();
        printService.shutdown();
        producer.close();
        consumer.close();
    }
}
