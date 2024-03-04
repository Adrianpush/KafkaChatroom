package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {

    private static final Scanner scanner = new Scanner(System.in);
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static User user;

    public static void main(String[] args) {
        while (readInput()) {
            logger.info("Enter command: ");
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
                logger.warn("Invalid input");
        }

        return true;
    }
}

