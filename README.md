# Sharded KV store

Розподілене key-value сховище: **координатор** (єдина публічна точка входу) + **шарди**
(зберігають дані в SQLite). Python 3.12, FastAPI.

```
          клієнт
            │  REST (openapi.yaml)
     ┌──────▼───────┐   каталог таблиць + реєстр шардів (catalog.json)
     │ coordinator  │   hash(table, pk) -> кільце consistent hashing -> шард
     └──┬────┬────┬─┘
        │    │    │      внутрішній HTTP
   ┌────▼┐ ┌─▼───┐ ┌▼────┐
   │ s-1 │ │ s-2 │ │ s-3 │   кожен шард сам реєструється: POST /shards
   └─────┘ └─────┘ └─────┘
```

## Запуск

```bash
pip install -r requirements-dev.txt

docker compose up -d --build                 # координатор + 2 шарди
docker compose --profile scale up -d shard3  # додати третій шард на льоту
```

## Приклад

```bash
# 1. зареєструвати таблицю
curl -X POST localhost:8000/tables -H 'content-type: application/json' \
     -d '{"name":"users","partition_key":"user_id"}'

# 2. CREATE / EXISTS / READ / UPDATE / DELETE
curl -X POST localhost:8000/tables/users/records -H 'content-type: application/json' \
     -d '{"pk":"alice","value":{"age":30}}'                       # 201, повторно — 409
curl 'localhost:8000/tables/users/records/exists?pk=alice'         # {"exists": true}
curl 'localhost:8000/tables/users/records?pk=alice'                # {"table":"users","pk":"alice","sk":null,"value":{"age":30}}
curl -X PUT localhost:8000/tables/users/records -H 'content-type: application/json' \
     -d '{"pk":"alice","value":{"age":31}}'                       # 404, якщо запису немає
curl -X DELETE 'localhost:8000/tables/users/records?pk=alice'      # 204

# 3. складений ключ: partition key + sort key
curl -X POST localhost:8000/tables -H 'content-type: application/json' \
     -d '{"name":"orders","partition_key":"customer_id","sort_key":"order_id"}'
curl -X POST localhost:8000/tables/orders/records -H 'content-type: application/json' \
     -d '{"pk":"c1","sk":"2024-01-15","value":{"total":99}}'
curl 'localhost:8000/tables/orders/query?pk=c1&sk_prefix=2024'

# 4. куди потрапить ключ / як розподілились дані
curl 'localhost:8000/route?table=users&pk=alice'
curl localhost:8000/stats
```

## Обмеження

* **Один координатор** — каталог і реєстр шардів зберігаються у файлі, тож після
  рестарту маршрутизація відновлюється.
* **Немає реплікації**: втрата диска шарда = втрата його даних. Недоступний шард → `503` для його ключів.
* Видалення шарда з кластера (decommission) не реалізоване — лише додавання.
