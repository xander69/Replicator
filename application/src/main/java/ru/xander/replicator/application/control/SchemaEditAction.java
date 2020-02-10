package ru.xander.replicator.application.control;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import lombok.Setter;
import ru.xander.replicator.application.controller.SchemaEditorController;
import ru.xander.replicator.application.entity.SchemaEntity;
import ru.xander.replicator.application.service.ViewService;


public class SchemaEditAction implements EventHandler<ActionEvent> {

    private final ViewService viewService;

    @Setter
    private SchemaEntity schemaEntity;

    public SchemaEditAction(ViewService viewService) {
        this.viewService = viewService;
    }

    @Override
    public void handle(ActionEvent event) {
        SchemaEditorController controller = viewService.loadView(SchemaEditorController.class, null);

//            Parent view = fxWeaver.loadView(SchemaEditorController.class);
//            Scene scene = new Scene(view);
//            Stage stage = new Stage();
//            stage.initOwner();
//            stage.initModality(Modality.APPLICATION_MODAL);
//            stage.setResizable(false);
//            stage.setScene(scene);
//            stage.setTitle("New Schema");
//            stage.show();
        controller.setSchemaEntity(schemaEntity);
        controller.show();
//        this.hide();
    }
}
