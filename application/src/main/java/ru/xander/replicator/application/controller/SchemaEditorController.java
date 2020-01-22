package ru.xander.replicator.application.controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;
import net.rgielen.fxweaver.core.FxmlView;
import org.springframework.stereotype.Component;
import ru.xander.replicator.schema.VendorType;

@Component
@FxmlView("/views/schemaEditor.fxml")
public class SchemaEditorController {
    @FXML
    public ComboBox<VendorType> chooseVendor;
    @FXML
    public TextField editJdbcDriver;
    @FXML
    public TextField editJdbcUrl;

    @FXML
    public void initialize() {
        ObservableList<VendorType> vendorTypes = FXCollections.observableArrayList(VendorType.values());
        chooseVendor.setItems(vendorTypes);
        chooseVendor.valueProperty().addListener((observable, oldValue, newValue) -> {
            this.editJdbcDriver.setText(newValue.getJdbcDriver());
            this.editJdbcUrl.setText(newValue.getUrlTemplate());
        });
    }
}
