mvn clean package
storm kill -w 0 normalApp
storm jar target/normalApp-1.0-jar-with-dependencies.jar com.github.dwladdimiroc.normalApp.topology.Topology constant