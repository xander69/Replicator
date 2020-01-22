package ru.xander.replicator.application;

import javafx.application.Application;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ReplicatorApplication {
    public static void main(String[] args) {
        Application.launch(ReplicatorFxApplication.class, args);
    }
}
