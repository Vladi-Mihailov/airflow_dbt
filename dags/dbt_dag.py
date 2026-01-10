
"""
DAG: Запуск dbt run и dbt test для модели my_first_dbt_model

- Сначала выполняет dbt run для модели my_first_dbt_model
- Затем выполняет тест accepted_values
- Затем выполняет тест not_null
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

# Путь к проекту dbt
DBT_PROJECT_DIR = "/opt/dbt/poligon"

# Connection ID для Telegram алертов
TELEGRAM_CONN_ID = "tg_alerting"


def send_telegram_alert(message: str, task_name: str, column_name: str):
    """
    Отправляет алерт в Telegram при провале теста
    """
    try:
        # Получаем connection для извлечения chat_id
        conn = BaseHook.get_connection(TELEGRAM_CONN_ID)
        chat_id = conn.extra_dejson.get("chat_id")
        
        if not chat_id:
            logger.warning("chat_id not found in connection extra, skipping alert")
            return
        
        # Формируем текст сообщения без Markdown для простоты
        text = (
            f"🚨 Ошибка в DAG: dbt_run_my_first_model\n\n"
            f"Задача: {task_name}\n"
            f"Столбец: {column_name}\n"
            f"Сообщение:\n{message}"
        )
        
        # Инициализируем hook с chat_id
        telegram_hook = TelegramHook(telegram_conn_id=TELEGRAM_CONN_ID, chat_id=chat_id)
        
        # Отправляем сообщение
        telegram_hook.send_message({"text": text})
        logger.info("✅ Алерт успешно отправлен в Telegram")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки алерта в Telegram: {e}")

default_args = {
    "retries": 0
}


@dag(
    dag_id="dbt_run_my_first_model",
    description="dbt run → accepted_values test → not_null test для модели my_first_dbt_model",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # запуск только вручную
    catchup=False,
    tags=["dbt", "poligon"],
    max_active_runs=1,
)
def dbt_my_first_model_runner():
    """
    DAG для запуска dbt run и dbt test модели my_first_dbt_model
    """

    # 1) Запуск dbt run для модели
    @task()
    def run_dbt_model():
        """
        Запускает dbt run для модели my_first_dbt_model
        """
        cmd = f"cd {DBT_PROJECT_DIR} && dbt run --select my_first_dbt_model"
        logger.info("Запуск dbt run для модели: my_first_dbt_model")
        logger.info("Команда: %s", cmd)
        
        try:
            completed = subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            logger.info("dbt run output (my_first_dbt_model):\n%s", completed.stdout)
            logger.info("✅ dbt run успешно выполнен для модели my_first_dbt_model")
        except subprocess.CalledProcessError as e:
            logger.error("❌ Ошибка dbt run для my_first_dbt_model: %s", e.stdout)
            raise

    # 2) Запуск теста accepted_values
    @task()
    def test_accepted_values():
        """
        Запускает тест accepted_values для модели my_first_dbt_model
        """
        # Запускаем только тесты типа accepted_values для модели my_first_dbt_model
        # Используем синтаксис: модель,test_name:accepted_values
        cmd = f"cd {DBT_PROJECT_DIR} && dbt test --select my_first_dbt_model,test_name:accepted_values"
        logger.info("Запуск теста accepted_values для модели: my_first_dbt_model")
        logger.info("Команда: %s", cmd)
        
        try:
            completed = subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            logger.info("dbt test accepted_values output:\n%s", completed.stdout)
            logger.info("✅ Тест accepted_values успешно выполнен")
        except subprocess.CalledProcessError as e:
            error_msg = f"Тест accepted_values провален для модели my_first_dbt_model\n\nОшибка:\n{e.stdout}"
            logger.error(f"❌ Ошибка теста accepted_values: {e.stdout}")
            send_telegram_alert(error_msg, "test_accepted_values", "id")
            raise

    # 3) Запуск теста not_null
    @task()
    def test_not_null():
        """
        Запускает тест not_null для модели my_first_dbt_model
        """
        # Запускаем только тесты типа not_null для модели my_first_dbt_model
        # Используем синтаксис: модель,test_name:not_null
        cmd = f"cd {DBT_PROJECT_DIR} && dbt test --select my_first_dbt_model,test_name:not_null"
        logger.info("Запуск теста not_null для модели: my_first_dbt_model")
        logger.info("Команда: %s", cmd)
        
        try:
            completed = subprocess.run(
                cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            logger.info("dbt test not_null output:\n%s", completed.stdout)
            logger.info("✅ Тест not_null успешно выполнен")
        except subprocess.CalledProcessError as e:
            error_msg = f"Тест not_null провален для модели my_first_dbt_model\n\nОшибка:\n{e.stdout}"
            logger.error(f"❌ Ошибка теста not_null: {e.stdout}")
            send_telegram_alert(error_msg, "test_not_null", "id")
            raise

    # Зависимости: run_dbt_model -> test_accepted_values -> test_not_null
    run_dbt_model() >> test_accepted_values() >> test_not_null()


# Создание экземпляра DAG
dag_instance = dbt_my_first_model_runner()
