package org.example.chatroom;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class ChatRoomManager {

    private static final ChatRoomManager instance = new ChatRoomManager();
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private final Set<String> chatRoomsNames;

    private ChatRoomManager() {
        chatRoomsNames = new HashSet<>();
    }

    public static synchronized ChatRoomManager getInstance() {
        return instance;
    }

    public boolean createChatroom(String chatroomName) {
        if (!chatRoomsNames.contains(chatroomName)) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            try (AdminClient adminClient = AdminClient.create(props)) {
                NewTopic newTopic = new NewTopic(chatroomName, 1, (short) 1);
                newTopic.configs(Collections.singletonMap("retention.ms", "86400000"));
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                return chatRoomsNames.add(chatroomName);
            } catch (InterruptedException | ExecutionException e) {
                throw new ChatroomCreateException("Unable to crate chatroom", e);
            }
        }
        return false;
    }

    public Set<String> getChatRoomsNames() {
        return new HashSet<>(chatRoomsNames);
    }

    public Consumer<String, String> createUserConsumer(String username) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, username);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public Producer<String, String> createUserProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public void createStream(String chatroomName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "meta");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        int numLines = 0;
        int numWords = 0;
        double averageWordsPerLine = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(new TopicPartition(chatroomName, 0)));
            consumer.seek(new TopicPartition(chatroomName, 0), 0);
            consumer.commitSync();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, String> kRecord : records) {
                    numLines++;
                    numWords += kRecord.value().split(" ").length;
                }
            }
            averageWordsPerLine = (double) numWords / Math.max(1, numLines);
            System.out.println("average words per line in chatroom " + chatroomName + " is " + averageWordsPerLine);
        } catch (Exception e) {
            System.err.println("Error in Kafka consumer: " + e.getMessage());
        }
    }
}
