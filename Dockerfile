# Используем официальный образ Apache Airflow версии 3.0.3
FROM apache/airflow:3.0.3
#1000-Это наш пользователь с хостовой машины, который будет запускать Airflow и DBT
#Если вы не знаете свой UID, то введите в терминале команду id и посмотрите на значение UID
USER root
# Установка Python и системных зависимостей
RUN apt-get update && apt-get install -y git

# Задаём доступы для юзеров airflow и default (UID 1000) для /opt/dbt и /opt/airflow
RUN mkdir -p /opt/dbt /opt/airflow/logs \
    && chown -R 1000:1000 /opt/dbt /opt/airflow \
    && chown -R airflow /opt/dbt /opt/airflow

# Копируем шаблон profiles.yml 
COPY --chown=airflow:root dbt/profiles.yml /opt/dbt/profiles.yml

# Копируем весь проект dbt в контейнер
COPY --chown=airflow:root dbt/poligon /opt/dbt/poligon

USER 1000
#установка редактора кода для Airflow DAGs
# Установка DBT и ClickHouse-зависимостей
RUN pip install --no-cache-dir \
    airflow-code-editor==8.0.1 \
    black isort fs-s3fs \
    dbt-core==1.10.4 \
    dbt-clickhouse==1.9.2 \
    airflow-clickhouse-plugin==1.5.0 \
    apache-airflow-providers-telegram         
