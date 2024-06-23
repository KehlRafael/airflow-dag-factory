# Airflow DAG Factory

This is a modified version of the [Official Apache Airflow 2.7.2 Docker Compose](https://airflow.apache.org/docs/apache-airflow/2.7.2/howto/docker-compose/index.html) to include [the DAG Factory](factory) into the project, as well as some example DAGs that were built using it.

This is not a production-ready container image and has many issues. The goal of this project is to provide a starting point for those that want to learn more about Airflow and DAG Factories.

## Prerequisites
This project requires only Docker to run.

To install Docker, please refer to their [official website installation guide](https://docs.docker.com/get-docker/). You can look into system specific installation guides below:
- macOS: [Install Docker Desktop](https://docs.docker.com/desktop/install/mac-install/)
- Linux/Ubuntu: [Install Docker Desktop](https://docs.docker.com/desktop/install/linux-install/) or [Install Docker Engine](https://docs.docker.com/engine/install/)
- Windows: [Install Docker Desktop](https://docs.docker.com/desktop/install/windows-install/)

With Docker installed, you are ready to start!

## Get started
First clone the repository into your local machine:
```
git clone https://github.com/KehlRafael/airflow-dag-factory.git
cd airflow-dag-factory
```

### Running the containers
To run the containers specified in the [docker-compose.yml](docker-compose.yml) file you can simply run:
```
docker compose -p airflow272_factory up -d
```
There is no need to build the image beforehand. All containers will be listed under the `airflow272_factory` project.

### Accessing the UI
To access the Airflow UI you must:
- Open the Airflow UI at [http://localhost:8080/](http://localhost:8080/)
- Username: airflow
- Password: airflow

These are the default values in the [docker-compose.yml](docker-compose.yml). These values can be overwritten by adding to [.env](.env) the variables `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD`, they will set the username and password, respectively.

### Stopping and removing images
To stop the containers run:
```
docker compose -p airflow272_factory stop
```
To tear down the environment run:
```
docker compose -p airflow272_factory down
```
You can add the `-v` and `--rmi local` flags at the end to also remove volumes and images, respectively, that are associated with the project.

### Contact
If you have any questions, concerns or want to suggest any improvements, please get in touch through my contact infomation availabe at my GitHub profile, [kehlrafael](https://github.com/kehlrafael).

### License
See the [LICENSE](LICENSE) file.
