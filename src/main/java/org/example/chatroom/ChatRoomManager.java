package org.example.chatroom;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ChatRoomManager {

    private ChatRoomManager instance;
    private Set<String> chatRoomsNames;

    private ChatRoomManager() {
        chatRoomsNames = new HashSet<>();
    }

    public static ChatRoomManager getInstance() {
        return new ChatRoomManager();
    }


    public boolean createChatroom(String chatroomName) {
        if (!chatRoomsNames.contains(chatroomName)) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectionDetails.BOOTSTRAP_SERVERS);

            try (AdminClient adminClient = AdminClient.create(props)) {
                NewTopic newTopic = new NewTopic(chatroomName, 1, (short) 1);
                newTopic.configs(Collections.singletonMap("retention.ms", "86400000"));
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic " + chatroomName + " created successfully.");
                return chatRoomsNames.add(chatroomName);
            } catch (InterruptedException | ExecutionException e) {
                throw new ChatroomCreateException("Unable to crate chatroom", e);
            }
        }
        return false;
    }
}
