package ru.xander.replicator.application.controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import lombok.Setter;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.stereotype.Component;
import ru.xander.replicator.application.entity.SchemaEntity;
import ru.xander.replicator.application.entity.SchemaVendor;


@Component
@FxmlView("/views/schemaEditor.fxml")
public class SchemaEditorController extends AbstractController {

    @FXML
    public TextField editName;
    @FXML
    public ComboBox<SchemaVendor> chooseVendor;
    @FXML
    public TextField editJdbcDriver;
    @FXML
    public TextField editJdbcUrl;
    @FXML
    public TextField editUsername;
    @FXML
    public PasswordField editPassword;
    @FXML
    public TextField editWorkSchema;
    @FXML
    public Button buttonTest;
    @FXML
    public Button buttonSave;
    @FXML
    public Button buttonCancel;

    @Setter
    private SchemaEntity schemaEntity;

    @FXML
    public void initialize() {
        ObservableList<SchemaVendor> vendorTypes = FXCollections.observableArrayList(SchemaVendor.values());
        chooseVendor.setItems(vendorTypes);
        chooseVendor.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue == null) {
                this.editJdbcDriver.setText(null);
                this.editJdbcUrl.setText(null);
            } else {
                this.editJdbcDriver.setText(newValue.getJdbcDriver());
                this.editJdbcUrl.setText(newValue.getUrlTemplate());
            }
        });
        buttonTest.setOnAction(event -> onTestClick());
        buttonSave.setOnAction(event -> onSaveClick());
        buttonCancel.setOnAction(event -> onCancelClick());
    }

    @Override
    public void show() {
        onShow();
        super.show();
    }

    private void onShow() {
        if (schemaEntity != null) {
            editName.setText(schemaEntity.getName());
            chooseVendor.setValue(schemaEntity.getVendor());
            editJdbcDriver.setText(schemaEntity.getJdbcDriver());
            editJdbcUrl.setText(schemaEntity.getJdbcUrl());
            editUsername.setText(schemaEntity.getUsername());
            editPassword.setText(schemaEntity.getPassword());
            editWorkSchema.setText(schemaEntity.getWorkSchema());
        } else {
            editName.setText(null);
            chooseVendor.setValue(null);
            editJdbcDriver.setText(null);
            editJdbcUrl.setText(null);
            editUsername.setText(null);
            editPassword.setText(null);
            editWorkSchema.setText(null);
        }
    }

    private void onTestClick() {
        // not implemented yet
    }

    private void onSaveClick() {
        if (schemaEntity == null) {
            schemaEntity = new SchemaEntity();
            State.getInstance().addSchema(schemaEntity);
        }

        //TODO: validate inputs
        schemaEntity.setName(editName.getText());
        schemaEntity.setVendor(chooseVendor.getValue());
        schemaEntity.setJdbcDriver(editJdbcDriver.getText());
        schemaEntity.setJdbcUrl(editJdbcUrl.getText());
        schemaEntity.setUsername(editUsername.getText());
        schemaEntity.setPassword(editPassword.getText());
        schemaEntity.setWorkSchema(editWorkSchema.getText());

//        schemaEventHandler.handle(new SchemaEditEvent(SchemaEditEvent.SchemaAction.SAVE, schemaEntity));

        this.hide();
    }

    private void onCancelClick() {
        this.hide();
    }
}
