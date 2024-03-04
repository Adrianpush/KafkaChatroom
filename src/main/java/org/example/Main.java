package org.example;

import org.example.chatroom.ChatRoomManager;

import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {

    private static User user;
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();

        // chatRoomManager.createChatroom("Evening");
        // chatRoomManager.createChatroom("Morning");
        while (readInput()) {
            System.out.println("Enter command: ");
        }
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
                user.sendMessage(Arrays.stream(userWords).skip(1).collect(Collectors.joining(" ", "", "e")));
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

