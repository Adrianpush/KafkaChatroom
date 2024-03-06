package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.example.chatroom.ChatRoomManager;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class User {

    String username;
    String currentChatRoom;
    Producer<String, String> producer;
    Consumer<String, String> consumer;
    TopicPartition topicPartition;
    ExecutorService printService;

    public User(String username) {
        this.username = username;
        consumer = ChatRoomManager.getInstance().createUserConsumer(username);
        producer = ChatRoomManager.getInstance().createUserProducer();
        this.printService = Executors.newSingleThreadExecutor();
    }

    public void joinChatRoom(String chatRoom, boolean fromStart) {
        try {
            topicPartition = new TopicPartition(chatRoom, 0);
            currentChatRoom = chatRoom;
            consumer.assign(List.of(topicPartition));
            if (fromStart) {
                resetOffset();
                System.out.println(consumer.position(topicPartition));
            }
            printService.submit(this::readNewMessages);
            System.out.println("Joined %s".formatted(chatRoom));
        } catch (RuntimeException e) {
            System.out.println("Cannot subscribe to chatroom: %s".formatted(e.getMessage()));
        }
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(currentChatRoom, username, message);
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

    public void leaveChatRoom() {
        currentChatRoom = null;
        System.out.println("Left chatroom");
    }

    public void logOut() {
        this.close();
    }

    private void resetOffset() {
        consumer.seek(topicPartition, 0);
        consumer.commitSync();
    }

    private void close() {
        leaveChatRoom();
        printService.shutdown();
        try {
            printService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        producer.close();
        consumer.close();
    }
}
