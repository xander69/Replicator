package ru.xander.replicator.application.control;

import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.util.Callback;
import lombok.extern.slf4j.Slf4j;
import ru.xander.replicator.application.entity.SchemaEntity;

@Slf4j
public class SchemaComboBoxCellFactory implements Callback<ListView<SchemaEntity>, ListCell<SchemaEntity>> {

//    public SchemaComboBoxCell callButtonCell() {
//        log.info("call button cell");
//        SchemaComboBoxCell buttonCell = call(null);
//        buttonCell.schemaPane.setPrefHeight(24.0);
//        return buttonCell;
//    }

    @Override
    public SchemaComboBoxCell call(ListView<SchemaEntity> param) {
        return new SchemaComboBoxCell();
    }

    public static class SchemaComboBoxCell extends ListCell<SchemaEntity> {
        private Pane schemaPane;

        @Override
        protected void updateItem(SchemaEntity schemaEntity, boolean empty) {
            super.updateItem(schemaEntity, empty);

            if (empty || (schemaEntity == null)) {
//                log.info("update empty");
                setText(null);
                setGraphic(null);
            } else {
//                log.info("update {}", schemaEntity.getName());
                if (schemaPane == null) {
//                    log.info("draw {}", schemaEntity.getName());
                    schemaPane = drawItem(schemaEntity);
                }
                setText(null);
                setGraphic(schemaPane);
            }
        }

        private Pane drawItem(SchemaEntity schemaEntity) {
            Pane pane = new Pane();
            pane.setPrefHeight(20.0);

            ImageView schemaLogo = new ImageView();
            schemaLogo.setFitWidth(16.0);
            schemaLogo.setFitHeight(16.0);
            schemaLogo.setLayoutX(12.0);
//            schemaLogo.setLayoutY(2.0);
            schemaLogo.imageProperty().bind(schemaEntity.vendorProperty());

            Label schemaName = new Label();
            schemaName.textProperty().bind(schemaEntity.getNameProperty());
            schemaName.setLayoutX(48.0);
            schemaName.setTextFill(Color.BLACK);
//            schemaName.setLayoutY(3.0);

            pane.getChildren().addAll(schemaLogo, schemaName);
            return pane;
        }
    }
}
