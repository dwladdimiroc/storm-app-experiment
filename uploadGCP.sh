mvn clean package
gcloud compute scp target/normalApp-1.0-jar-with-dependencies.jar sps-storm-central-backup-1:~/scripts/App-4-Fix.jar