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
    Откройте файл config.json и укажите свои данные для подключения к ClickHouse, имя таблицы.
    
    Пример config.json с table_definition:

    ```json
    {
      "clickhouse_user": "default",
      "clickhouse_password": "",
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
    ### Пояснения к параметрам

    - `clickhouse_user`, `clickhouse_password`, `clickhouse_host`, `clickhouse_port`:  
      Установите в соответствии с вашей настройкой ClickHouse.  
      Для Docker-контейнера по умолчанию:  
      `user`: `default`, `password`: `""`, `host`: `localhost`, `port`: `9000`.
    
    - `table_name`:  
      Имя таблицы, в которую будут вставляться данные.
    
    - `total_inserts`:  
      Общее количество строк для генерации и вставки.
    
    - `inserts_per_query`:  
      Количество строк, вставляемых за один пакетный запрос.
    
    - `generation_seed`:  
      Целое число для инициализации генератора случайных чисел для воспроизводимых наборов данных.  
      Если `null`, данные будут отличаться при каждом запуске.
    
    - `hints`:  
      Словарь для предоставления подсказок по генерации для конкретных столбцов.
    
      **Форматы подсказок:**
    
      - Для числовых столбцов: `[min, max]` (диапазон) или `[val1, val2, ...]` (список значений).
      - Для строковых столбцов: `["val1", "val2", ...]` (список значений).
      - Для даты/времени: `{"start": "YYYY-MM-DD HH:MM:SS", "end": "YYYY-MM-DD HH:MM:SS"}`.


3.  **Запустите скрипт:**
    Убедитесь, что целевая таблица уже создана в ClickHouse.
    ```bash
    python src/main.py
    ```
