package ru.xander.replicator.application.control;

import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Pane;
import javafx.util.Callback;
import lombok.extern.slf4j.Slf4j;
import ru.xander.replicator.application.controller.MainController;
import ru.xander.replicator.application.controller.SchemaEditorController;
import ru.xander.replicator.application.controller.State;
import ru.xander.replicator.application.entity.SchemaEntity;

@Slf4j
public class SchemaListViewCellFactory implements Callback<ListView<SchemaEntity>, ListCell<SchemaEntity>> {

    private final MainController mainController;

    public SchemaListViewCellFactory(MainController mainController) {
        this.mainController = mainController;
    }

    @Override
    public SchemaListViewCell call(ListView<SchemaEntity> param) {
        return new SchemaListViewCell(mainController);
    }

    public static class SchemaListViewCell extends ListCell<SchemaEntity> {
        private final MainController mainController;
        private Pane schemaPane;

        public SchemaListViewCell(MainController mainController) {
            this.mainController = mainController;
        }

        @Override
        protected void updateItem(SchemaEntity schemaEntity, boolean empty) {
            log.info("update {}", schemaEntity == null ? "null" : schemaEntity.getId());
            super.updateItem(schemaEntity, empty);
            if (empty || (schemaEntity == null)) {
                setText(null);
                setGraphic(null);
            } else {
                if (schemaPane == null) {
                    schemaPane = drawItem(schemaEntity);
                }
                setText(null);
                setGraphic(schemaPane);
            }
        }

        private Pane drawItem(SchemaEntity schemaEntity) {
            Pane pane = new Pane();
            pane.setPrefWidth(245.0);
            pane.setPrefHeight(26.0);

            ImageView schemaLogo = new ImageView();
            schemaLogo.setFitWidth(16.0);
            schemaLogo.setFitHeight(16.0);
            schemaLogo.setLayoutX(8.0);
            schemaLogo.setLayoutY(6.0);
            schemaLogo.imageProperty().bind(schemaEntity.vendorProperty());

            Label schemaName = new Label();
            schemaName.setLayoutX(36.0);
            schemaName.setLayoutY(5.0);
            schemaName.textProperty().bind(schemaEntity.nameProperty());

            Hyperlink schemaEdit = new Hyperlink();
            schemaEdit.setText("Edit");
            schemaEdit.setLayoutX(168.0);
            schemaEdit.setLayoutY(1.0);
            schemaEdit.setOnAction(event -> {
                log.info("Edit schema {} (Id: {})", schemaEntity.getName(), schemaEntity.getId());
                SchemaEditorController controller = mainController.loadView(SchemaEditorController.class);
                controller.setSchemaEntity(schemaEntity);
//            controller.setSchemaEventHandler(this);
                controller.show();

//            schemaEventHandler.handle(new SchemaEditEvent(SchemaEditEvent.SchemaAction.EDIT, schemaEntity))
            });

            Hyperlink schemaDelete = new Hyperlink();
            schemaDelete.setText("Delete");
            schemaDelete.setLayoutX(195.0);
            schemaDelete.setLayoutY(1.0);
            schemaDelete.setOnAction(event -> {
                State.getInstance().delSchema(schemaEntity);
//            schemaEventHandler.handle(new SchemaEditEvent(SchemaEditEvent.SchemaAction.DELETE, schemaEntity))
            });

            pane.getChildren().addAll(schemaLogo, schemaName, schemaEdit, schemaDelete);
            return pane;
        }
    }
}
