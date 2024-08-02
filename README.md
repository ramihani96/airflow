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

1. **Download the `docker-compose.yaml` File**

   The `docker-compose.yaml` file defines the services required to run Apache Airflow. Download it for the desired version. In this example, we'll use version 2.9.1:

   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
