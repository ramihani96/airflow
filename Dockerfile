# # Start from the Airflow base image
FROM apache/airflow:2.9.1

# Switch to root user to install additional packages
USER root

# Update package list and install packages
RUN apt-get update && \
    apt-get install -y postgresql-client vim nano && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Switch back to the airflow user
USER airflow

# Install Airflow requirements from requirements.txt
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt


EXPOSE 8080
EXPOSE 8793
EXPOSE 5555