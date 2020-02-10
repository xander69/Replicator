package ru.xander.replicator.application;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import net.rgielen.fxweaver.core.FxControllerAndView;
import net.rgielen.fxweaver.core.FxWeaver;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import ru.xander.replicator.application.controller.MainController;

public class ReplicatorFxApplication extends Application {

    private ConfigurableApplicationContext applicationContext;

    @Override
    public void init() {
        String[] args = getParameters().getRaw().toArray(new String[0]);
        this.applicationContext = new SpringApplicationBuilder()
                .sources(ReplicatorApplication.class)
                .run(args);
    }

    @Override
    public void start(Stage primaryStage) {
        FxWeaver fxWeaver = applicationContext.getBean(FxWeaver.class);
        FxControllerAndView<MainController, Node> mainControllerAndView = fxWeaver.load(MainController.class);
        Parent root = (Parent) mainControllerAndView.getView().get();
        Scene scene = new Scene(root);
        primaryStage.setScene(scene);
        primaryStage.show();
        mainControllerAndView.getController().setScene(scene);
    }

    @Override
    public void stop() {
        this.applicationContext.close();
        Platform.exit();
    }
}
