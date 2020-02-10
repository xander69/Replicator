package ru.xander.replicator.application.service;

import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import net.rgielen.fxweaver.core.FxControllerAndView;
import net.rgielen.fxweaver.core.FxWeaver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.xander.replicator.application.controller.AbstractController;

import java.util.HashMap;
import java.util.Map;

@Service
@Deprecated
public class ViewService {

    private final FxWeaver fxWeaver;
    private final Map<Class<? extends AbstractController>, AbstractController> controllerCache;

    @Autowired
    public ViewService(FxWeaver fxWeaver) {
        this.fxWeaver = fxWeaver;
        this.controllerCache = new HashMap<>();
    }

    public <T extends AbstractController> T loadView(Class<T> controllerClass, Window owner) {
        if (controllerCache.containsKey(controllerClass)) {
            return (T) controllerCache.get(controllerClass);
        }
        FxControllerAndView<? extends AbstractController, Node> controllerAndView = fxWeaver.load(controllerClass);
        AbstractController controller = controllerAndView.getController();

        Parent view = fxWeaver.loadView(controllerClass);
        Scene scene = new Scene(view);
        Stage stage = new Stage();
        stage.initOwner(owner);
        stage.initModality(Modality.APPLICATION_MODAL);
        stage.setResizable(false);
        stage.setScene(scene);
        controller.setStage(stage);

        controllerCache.put(controllerClass, controller);
        return (T) controller;
    }
}
