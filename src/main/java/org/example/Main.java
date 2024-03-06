package org.example;

import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {

    private static final Scanner scanner = new Scanner(System.in);
    private static User user = new User("Anonymous");

    public static void main(String[] args) {
//        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();
//        if(chatRoomManager.getChatRoomsNames().isEmpty()) {
//            chatRoomManager.createChatroom("A");
//            chatRoomManager.createChatroom("B");
//            chatRoomManager.createChatroom("C");
//        }

        boolean flag = true;
        printInstructions();
        while (flag) {
            flag = readInput();
        }

//        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();
//        chatRoomManager.createStream("A");
    }

    private static boolean readInput() {
        String userInput = scanner.nextLine();
        String[] userWords = userInput.split(" ");
        String command = userWords[0];
        switch (command) {
            case "exit":
                user.logOut();
                scanner.close();
                return false;
            case "leave":
                user.leaveChatRoom();
                break;
            case "login":
                user.logOut();
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

    private static void printInstructions() {
        System.out.println(
                """
                        ***INSTRUCTIONS***
                        To log in on a custom User type login followed by the username.
                        To join a chatroom type join followed by the chatroom name. 
                        To join a chatroom and print all previous messages type enter followed by the chatroom name.
                        To send a message type send followed by the message after joining a chatroom.
                        To leave a chatroom type leave. 
                        To exit the program type exit.
                        ***"""
        );
    }
}

