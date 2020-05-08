package ru.xander.replicator.application.controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.rgielen.fxweaver.core.FxControllerAndView;
import net.rgielen.fxweaver.core.FxWeaver;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.xander.replicator.application.control.SchemaComboBoxCellFactory;
import ru.xander.replicator.application.entity.SchemaEntity;
import ru.xander.replicator.application.repository.SchemaEntityRepo;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
@Slf4j
@Component
@FxmlView("/views/main.fxml")
public class MainController {

    private final FxWeaver fxWeaver;
    private final SchemaEntityRepo schemaEntityRepo;
    @FXML
    public TextArea logContent;
//    @Getter
//    private ObservableList<SchemaEntity> schemas;

    @FXML
    public Button buttonSchemas;
    @FXML
    public ComboBox<SchemaEntity> chooseSource;
    @FXML
    public ComboBox<SchemaEntity> chooseTarget;
    @FXML
    public ListView<String> listSourceTables;
    @FXML
    public ListView<String> listTargetTables;
    @FXML
    public Button buttonUpdateSource;
    @FXML
    public Button buttonUpdateTarget;

    @Setter
    private Scene scene;

    @Autowired
    public MainController(FxWeaver fxWeaver, SchemaEntityRepo schemaEntityRepo) {
        this.fxWeaver = fxWeaver;
        this.schemaEntityRepo = schemaEntityRepo;
    }

    private ObservableList<String> tables;

    @FXML
    public void initialize() {
//        buttonNewSchema.setOnAction(event -> {
//            Parent view = fxWeaver.loadView(SchemaEditorController.class);
//            Scene scene = new Scene(view);
//            Stage stage = new Stage();
//            stage.initOwner(((Node) event.getSource()).getScene().getWindow());
//            stage.initModality(Modality.APPLICATION_MODAL);
//            stage.setResizable(false);
//            stage.setScene(scene);
//            stage.setTitle("New Schema");
//            stage.show();
//        });

        log.info("Load Main Controller");

//        State.getInstance().getSchemas().addAll(schemaEntityRepo.findAll());

        chooseSource.setItems(State.getInstance().getSchemas());
        chooseTarget.setItems(State.getInstance().getSchemas());

        SchemaComboBoxCellFactory sourceSchemasFactory = new SchemaComboBoxCellFactory();
        SchemaComboBoxCellFactory targetSchemasFactory = new SchemaComboBoxCellFactory();
        chooseSource.setCellFactory(sourceSchemasFactory);
        chooseSource
                .getSelectionModel()
                .selectedItemProperty()
                .addListener((observable, oldValue, newValue) -> chooseSource.setButtonCell(sourceSchemasFactory.call(null)));
        chooseTarget.setCellFactory(targetSchemasFactory);
        chooseTarget
                .getSelectionModel()
                .selectedItemProperty()
                .addListener((observable, oldValue, newValue) -> chooseTarget.setButtonCell(targetSchemasFactory.call(null)));

        buttonSchemas.setOnAction(event -> {
            SchemaListController controller = loadView(SchemaListController.class);
            controller.show();
        });

        tables = FXCollections.observableArrayList();
        buttonUpdateSource.setOnAction(event -> tables.add("SOURCE-" + tables.size()));
        buttonUpdateTarget.setOnAction(event -> tables.add("TARGET-" + tables.size()));

        listSourceTables.setItems(tables);
        listTargetTables.setItems(tables);
    }


    private final Map<Class<? extends AbstractController>, AbstractController> controllerCache = new HashMap<>();
    @SuppressWarnings("unchecked")
    public <T extends AbstractController> T loadView(Class<T> controllerClass) {
        if (controllerCache.containsKey(controllerClass)) {
            return (T) controllerCache.get(controllerClass);
        }
        FxControllerAndView<? extends AbstractController, Node> controllerAndView = fxWeaver.load(controllerClass);
        AbstractController controller = controllerAndView.getController();

        Parent view = fxWeaver.loadView(controllerClass);
        Stage stage = new Stage(StageStyle.UTILITY);
        stage.initOwner(this.scene.getWindow());
        stage.initModality(Modality.APPLICATION_MODAL);
        stage.setResizable(false);
        stage.setScene(new Scene(view));

        controller.setMainController(this);
        controller.setStage(stage);

        controllerCache.put(controllerClass, controller);
        return (T) controller;
    }
}
