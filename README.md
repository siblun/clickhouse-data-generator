# Генератор тестовых данных для ClickHouse

Этот инструмент представляет собой Python-скрипт для автоматической генерации и вставки больших объемов тестовых данных в таблицы ClickHouse. Он позволяет гибко настраивать генерируемые значения, их количество и формат.

## Установка

1.  **Клонируйте репозиторий:**
    ```bash
    git clone [https://github.com/your-username/clickhouse-data-generator.git](https://github.com/your-username/clickhouse-data-generator.git)
    cd clickhouse-data-generator
    ```

2.  **Создайте и активируйте виртуальное окружение:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Для Windows: venv\Scripts\activate
    ```

3.  **Установите зависимости:**
    ```bash
    pip install -r requirements.txt
    ```

## Настройка и запуск

1.  **Отредактируйте `config.json`:**
    Откройте файл `config.json` и укажите свои данные для подключения к ClickHouse, имя таблицы и параметры генерации.

    ```json
    {
      "clickhouse_user": "default",
      "clickhouse_password": "your_password",
      "clickhouse_host": "localhost",
      "clickhouse_port": 9000,
      "table_name": "users",
      "total_inserts": 10000,
      "inserts_per_query": 1000,
      "generation_seed": 42,
      "hints": {
        "age": [18, 75],
        "name": ["Alice", "Bob", "Charlie"]
      }
    }
    ```

2.  **Запустите скрипт:**
    Убедитесь, что целевая таблица (`users` в примере) уже создана в ClickHouse.
    ```bash
    python src/main.py
    ```
