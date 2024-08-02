# Installing Apache Airflow Locally
This guide provides step-by-step instructions for setting up Apache Airflow on your local machine using Docker.

## Prerequisites
Before you begin, ensure the following are installed on your system:

- **Docker**: Docker is required to run Airflow in containers. You can download and install Docker from [Docker's official website](https://www.docker.com/get-started).

  - **For Ubuntu (Linux)**: Follow the installation guide [here](https://docs.docker.com/engine/install/ubuntu/).
  - **Post-Installation Steps**: After installing Docker, it is recommended to add your user to the Docker group to avoid using `sudo` with each Docker command. Replace `{user_name}` with your actual username:

    ```bash
    sudo usermod -aG docker {user_name}
    ```

    After running the above command, log out and log back in for the changes to take effect.

- **Docker Compose**: Docker Compose is used to define and manage multi-container Docker applications. Install Docker Compose from [Docker's official documentation](https://docs.docker.com/compose/install/linux/).

  - **Installation Command**:

    ```bash
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.29.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    ```

## Install Apache Airflow
Follow these steps to set up Apache Airflow using Docker Compose:

Follow these steps to set up Apache Airflow using Docker Compose:

1. **Download the `docker-compose.yaml` File**

   The `docker-compose.yaml` file defines the services required to run Apache Airflow. Download it for the desired version. In this example, we'll use version 2.9.1:

    ```bash
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
    ```

2. **Add a `requirements.txt` File**

   Create a `requirements.txt` file for needed libraries to be installed. Place this file in the same directory as your `docker-compose.yaml` file. Example content for `requirements.txt`:

    ```txt
    pandas==1.3.3
    requests==2.26.0
    ```

3. **Create a Custom Dockerfile**

   Create a `Dockerfile` to customize the Airflow image. Example `Dockerfile`:

    ```Dockerfile
    FROM apache/airflow:2.9.1
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt
    ```

   Build the Docker image with the following command, replacing `{image_name}` with your desired image name:

    ```bash
    docker build -t {image_name} .
    ```

   Example:

    ```bash
    docker build -t airflow-rami:latest .
    ```

4. **Update `docker-compose.yaml`**

   Update the `docker-compose.yaml` file to fetch the image name you just built. Example `docker-compose.yaml` file update:

    ```yaml
    x-airflow-common:
    &airflow-common
    image: ${AIRFLOW_IMAGE_NAME:-airflow-rami:latest}
        ...
    ```

5. **Add Volumes for `src` and `queries`**

   Update the `docker-compose.yaml` file to include volumes for the `src` and `queries` folders containing both python & SQL scripts. Example:

    ```yaml
    volumes:
      - ./src:/opt/airflow/src
      - ./queries:/opt/airflow/queries
    ```

6. **Add `airflow.cfg` File**

   Create an `airflow.cfg` file containing all Airflow configuration settings and place it in the appropriate directory. Example `docker-compose.yaml` update:

7. **Start Airflow**

   Use Docker Compose to start Airflow:

    ```bash
    docker-compose up -d
    ```

    This command will start all the services defined in your `docker-compose.yaml` file.


8. **User Interface**

   Access the user interface at the following link: [http://localhost:8080](http://localhost:8080). The initial login credentials for Airflow are:

   - **Username**: `airflow`
   - **Password**: `airflow`

   You can change these credentials as needed.


9. **Portainer**

   Portainer is a UI tool used to monitor all running containers, images, volumes, and other Docker-related components on your machine. Follow these steps to set up Portainer:

   - **Create a Volume for Portainer**:

     ```bash
     docker volume create portainer_data
     ```

   - **Download and Install Portainer**:

     ```bash
     docker run -d -p 8000:8000 -p 9443:9443 --name portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ee:latest
     ```

   - **Login from Browser**:

     Open your browser and navigate to [https://localhost:9443](https://localhost:9443) to access the Portainer UI.

This guide provides clear, step-by-step instructions for setting up Apache Airflow locally using Docker and Docker Compose, along with monitoring your Docker environment using Portainer.