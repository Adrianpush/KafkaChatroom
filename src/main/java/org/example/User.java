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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class User {

    String name;
    String currentChatRoom;
    Producer<String, String> producer;
    Consumer<String, String> consumer;
    TopicPartition topicPartition;
    ExecutorService printService;
    Logger logger = LoggerFactory.getLogger(User.class);

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
            logger.info("Joined %s".formatted(chatRoom));
        } catch (RuntimeException e) {
            logger.warn("Cannot subscribe to chatroom: %s".formatted(e.getMessage()));
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
                    logger.info(kafkaRecord.topic() + " - " + kafkaRecord.key() + ": " + kafkaRecord.value()));
            consumer.commitSync();
        }
    }

    private void resetOffset() {
        consumer.seek(topicPartition, 0);
        consumer.commitSync();
    }

    public void leaveChatRoom() {
        currentChatRoom = null;
        logger.info("Left chatroom");
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
