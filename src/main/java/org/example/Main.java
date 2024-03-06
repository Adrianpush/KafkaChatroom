package org.example;

import org.example.chatroom.ChatRoomManager;

import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {

    private static final Scanner scanner = new Scanner(System.in);
    private static User user;

    public static void main(String[] args) {
//        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();
//        if(chatRoomManager.getChatRoomsNames().isEmpty()) {
//            chatRoomManager.createChatroom("A");
//            chatRoomManager.createChatroom("B");
//            chatRoomManager.createChatroom("C");
//        }
        while (readInput()) {
            System.out.println("Enter command: ");
        }
        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();
        chatRoomManager.createStream("A");
    }

    private static boolean readInput() {
        String userInput = scanner.nextLine();
        String[] userWords = userInput.split(" ");
        String command = userWords[0];
        switch (command) {
            case "exit":
                if (user != null) user.logOut();
                scanner.close();
                return false;
            case "leave":
                user.leaveChatRoom();
                break;
            case "login":
                user = new User(userWords[1]);
                break;
            case "send":
                user.sendMessage(Arrays.stream(userWords).skip(1).collect(Collectors.joining(" ", "", " ")));
                break;
            case "join":
                user.joinChatRoom(userWords[1], false);
                break;
            case "enter":
                user.joinChatRoom(userWords[1], true);
                break;
            default:
                System.out.println("Invalid input");
        }
        return true;
    }
}

