package ru.xander.replicator.application.controller;

import javafx.stage.Stage;
import lombok.Setter;

public abstract class AbstractController {

    @Setter
    protected MainController mainController;
    @Setter
    private Stage stage;

    public void show() {
        stage.show();
    }

    public void showAndWait() {
        stage.showAndWait();
    }

    public void hide() {
        stage.hide();
    }
}
