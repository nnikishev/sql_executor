# SQL Executor

C++ модуль с Python биндингами для выполнения SQL запросов в PostgreSQL и ClickHouse.

## Быстрый старт

# PostgresConnector

```
# Конструктор
pg = se.PostgresConnector()

# Основные методы
pg.connect(conninfo)                      # Подключение к PostgreSQL
pg.disconnect()                          # Отключение от PostgreSQL
pg.is_connected() -> bool                # Проверка подключения
pg.execute(query) -> dict                # Выполнение SQL запроса

# Методы управления транзакциями
pg.begin_transaction() -> bool           # Начало транзакции
pg.commit_transaction() -> bool          # Подтверждение транзакции  
pg.rollback_transaction() -> bool        # Откат транзакции
pg.is_in_transaction() -> bool           # Проверка активности транзакции
pg.execute_batch(queries) -> bool        # Пакетное выполнение запросов
```

## Использование
```python
import sql_executor as se

# Проверка работы модуля
print(se.test())  # "SQL Executor module is working!"

# Создание и подключение
pg = se.PostgresConnector()
pg.connect("host=localhost dbname=test user=postgres password=password")

# Выполнение запроса
result = pg.execute("SELECT * FROM users WHERE age > 18")

# Транзакции
pg.begin_transaction()
pg.execute_batch([
    "UPDATE accounts SET balance = balance - 100 WHERE id = 1",
    "UPDATE accounts SET balance = balance + 100 WHERE id = 2"
])
pg.commit_transaction()

# Отключение
pg.disconnect()
```


# ClickHouse Connector

```
# Конструктор
ch = se.ClickHouseConnector()

# Основные методы
ch.connect(host, port, database='default', user='default', password='') -> bool
ch.disconnect()                          # Отключение от ClickHouse
ch.is_connected() -> bool                # Проверка подключения  
ch.execute(query) -> dict                # Выполнение SQL запроса
```

## Использование
```python 
import sql_executor as se

# Создание и подключение
ch = se.ClickHouseConnector()
ch.connect("localhost", 8123, "default", "default", "")

# Выполнение запроса
result = ch.execute("""
    SELECT 
        user_id,
        event_name,
        created_at
    FROM events 
    WHERE date = today()
    LIMIT 100
""")

# Отключение
ch.disconnect()
```

# Формат ответа

```json
{
    "rows": [
        {
            "id": "1",
            "name": "John Doe", 
            "email": "john@example.com",
            "age": "30",
            "salary": "50000.00",
            "created_at": "2023-01-15 10:30:00",
            "nullable_field": None  # NULL значения как Python None
        },
        {
            "id": "2",
            "name": "Jane Smith",
            "email": "jane@example.com", 
            "age": "25",
            "salary": "60000.50",
            "created_at": "2023-02-20 14:45:00",
            "nullable_field": "some_value"
        }
    ],
    "columns": [
        {"name": "id", "type": "int4"},
        {"name": "name", "type": "text"},
        {"name": "email", "type": "varchar"},
        {"name": "age", "type": "int4"},
        {"name": "salary", "type": "numeric"},
        {"name": "created_at", "type": "timestamp"},
        {"name": "nullable_field", "type": "text"}
    ],
    "count": 150  # Общее количество строк (без учета LIMIT)
}
```