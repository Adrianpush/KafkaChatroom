package org.example;

import org.example.chatroom.ChatRoomManager;

import java.util.Scanner;

public class Main {

    private static User user;
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        ChatRoomManager chatRoomManager = ChatRoomManager.getInstance();

        // chatRoomManager.createChatroom("Evening");
        // chatRoomManager.createChatroom("Morning");
        while (readInput()) {

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
            case "login":
                user = new User(userWords[1]);
                break;
            case "send":
                user.sendMessage(userWords[1]);
                break;
            case "join":
                user.joinChatRoom(userWords[1]);
                break;
            default:
                System.out.println("Invalid input");
        }

        return true;
    }
}

