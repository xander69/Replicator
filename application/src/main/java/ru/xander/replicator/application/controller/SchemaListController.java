package ru.xander.replicator.application.controller;

import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.ListView;
import lombok.extern.slf4j.Slf4j;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.xander.replicator.application.control.SchemaEditEvent;
import ru.xander.replicator.application.control.SchemaListViewCellFactory;
import ru.xander.replicator.application.entity.SchemaEntity;
import ru.xander.replicator.application.repository.SchemaEntityRepo;

@Slf4j
@Component
@FxmlView("/views/schemaList.fxml")
public class SchemaListController extends AbstractController implements EventHandler<SchemaEditEvent> {

    @FXML
    public ListView<SchemaEntity> listSchema;
    @FXML
    public Hyperlink addNewSchema;

    private final SchemaEntityRepo schemaEntityRepo;

//    @Setter
//    private ObservableList<SchemaEntity> schemas;

    @Autowired
    public SchemaListController(SchemaEntityRepo schemaEntityRepo) {
        this.schemaEntityRepo = schemaEntityRepo;
    }

    @FXML
    public void initialize() {
        log.info("Init schema list controller");
//        ObservableList<SchemaEntity> schemas = FXCollections.observableArrayList(schemaEntityRepo.findAll());
        listSchema.setItems(State.getInstance().getSchemas());
//        listSchema.itemsProperty().bind(State.getInstance().getSchemas());
        listSchema.setCellFactory(new SchemaListViewCellFactory(mainController));
        addNewSchema.setOnAction(event -> editSchema(null));
    }

    @Override
    public void handle(SchemaEditEvent event) {
//        SchemaEntity schemaEntity = event.getSchemaEntity();
//        switch (event.getAction()) {
//            case SAVE:
//                saveSchema(schemaEntity);
//                break;
//            case EDIT:
//                editSchema(schemaEntity);
//                break;
//            case DELETE:
//                deleteSchema(schemaEntity);
//                break;
//        }
    }

    private void saveSchema(SchemaEntity schemaEntity) {
        log.info("Save schema {} (Id: {})", schemaEntity.getName(), schemaEntity.getId());
        SchemaEntity savedSchemaEntity = schemaEntityRepo.save(schemaEntity);
        if (schemaEntity.getId() == null) {
            State.getInstance().getSchemas().add(savedSchemaEntity);
        }
//        listSchema.setItems(schemas);
//        listSchema.getCellFactory().call(null);
    }

    private void editSchema(SchemaEntity schemaEntity) {
        if (schemaEntity == null) {
            log.info("Add new schema");
        } else {
            log.info("Edit schema {} (Id: {})", schemaEntity.getName(), schemaEntity.getId());
        }
        SchemaEditorController controller = mainController.loadView(SchemaEditorController.class);
        controller.setSchemaEntity(schemaEntity);
        controller.show();
    }

    private void deleteSchema(SchemaEntity schemaEntity) {
        log.info("Delete schema {} (Id: {})", schemaEntity.getName(), schemaEntity.getId());
        schemaEntityRepo.delete(schemaEntity);
        State.getInstance().getSchemas().remove(schemaEntity);
    }

    //    @Override
//    public void handle(ActionEvent event) {
//        SchemaEditorController controller = viewService.loadView(SchemaEditorController.class, null);
////            Parent view = fxWeaver.loadView(SchemaEditorController.class);
////            Scene scene = new Scene(view);
////            Stage stage = new Stage();
////            stage.initOwner();
////            stage.initModality(Modality.APPLICATION_MODAL);
////            stage.setResizable(false);
////            stage.setScene(scene);
////            stage.setTitle("New Schema");
////            stage.show();
//        controller.setSchemaEntity(schemaEntity);
//        controller.show();
//        this.hide();
//    }
}
