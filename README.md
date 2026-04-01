# RTF Parser For FLS Statements

Асинхронный микросервис для разбора `.rtf`-выписок по лицевому счету о задолженности по квартплате и коммунальным услугам.

Сервис:
- принимает `.rtf` через HTTP
- извлекает шапку документа (`Ф.И.О.`, `Адрес`)
- нормализует адрес на составные части
- разбирает помесячные начисления и итоговые строки
- выполняет обязательные математические проверки
- работает асинхронно через очередь задач и фоновые воркеры


## Структура результата

На выходе сервис возвращает JSON с полями:
- `document_type`
- `statement_title`
- `source_filename`
- `account_holder_name`
- `address_raw`
- `address`
- `charges`
- `year_totals`
- `grand_total`
- `validations`
- `parsing`

`address` содержит нормализованные части адреса в стиле `egd-parser`:
- `raw`
- `street`
- `house`
- `building`
- `structure`
- `apartment`
- `full`

`charges` содержит месячные строки с каноническими полями:
- `month`
- `year`
- `maintenance_housing`
- `rent`
- `heating_main`
- `hot_water`
- `cold_water`
- `cold_water_for_hot_water`
- `sewerage`
- `radio_and_alert`
- `antenna`
- `locking_device`
- `gas`
- `capital_repair`
- `solid_waste`
- `other_services`
- `total_accrued`
- `adjustment`
- `paid`
- `debt_total`


## Математические проверки

Сервис всегда возвращает блок `validations` с результатами сверок:
- `total_accrued_equals_sum_of_services`
- `debt_total_equals_total_plus_adjustment_minus_paid`
- `year_total_matches_sum_of_monthly_rows`
- `grand_total_matches_sum_of_monthly_rows`

Внутри:
- `is_valid`
- `checks_total`
- `checks_passed`
- `checks_failed`
- `checks`

Каждая проверка содержит:
- `scope`
- `rule`
- `passed`
- `actual`
- `expected`
- `delta`


## API

База: `http://localhost:8000`

- `POST /api/convert`
  - `multipart/form-data`
  - поле `file`: `.rtf`
  - query `callback`: опционально
- `GET /api/convert/{id}/status`
- `GET /api/convert/{id}/cancel`
- `POST /api/convert/{id}`
- `GET /health`


## Переменные окружения

- `STORAGE_DIR` — каталог для результатов и временных файлов, по умолчанию `/data`
- `RETENTION_SECONDS` — срок хранения завершенных задач, по умолчанию `3600`
- `WORKERS` — количество фоновых воркеров, по умолчанию `2`
- `LOG_LEVEL` — уровень логирования, по умолчанию `INFO`
- `CALLBACK_URL` — базовый callback, если не передан в запросе
- `CALLBACK_TIMEOUT_SECONDS` — timeout callback-запроса, по умолчанию `20`
- `MAX_FILE_SIZE_BYTES` — лимит загружаемого `.rtf`, по умолчанию `10485760`
- `ALLOWED_ORIGINS` — список CORS origin через запятую


## Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```


## Тесты

```bash
python -m unittest tests/test_parser.py
```

Текущий набор тестов проверяет оба предоставленных `.rtf`-образца:
- извлечение `Ф.И.О.` и `Адрес`
- нормализацию адреса на `street` / `house` / `building` / `apartment`
- количество месячных строк и годовых итогов
- наличие общего итога
- прохождение всех математических проверок


## Docker

```bash
docker build -t fls-rtf-parser:latest .
docker run --rm -p 8000:8000 -v $(pwd)/data:/data fls-rtf-parser:latest
```

Или через `docker-compose`:

```bash
cp .env.example .env
docker compose up -d --build
```


## Прод

- Не храните в репозитории входные `.rtf`, выходные `.json` и временные каталоги задач
- Для контейнера обязательно монтируйте отдельный volume в `/data`
- Перед продом задайте `ALLOWED_ORIGINS` только доверенными доменами
- Для внешнего контура лучше ставить сервис за reverse proxy с лимитами на размер upload
- Метаданные задач хранятся в памяти, поэтому после рестарта статусы очереди не восстанавливаются
- В контейнере сервис запускается не от `root`
- Используйте `.env` на основе `.env.example` и не храните секретные callback URL в git
- Зависимости в `requirements.txt` зафиксированы по версиям для воспроизводимой сборки


## Ограничения текущей версии

- Парсер ориентирован на формат выписок, аналогичный уже проверенным образцам
- Табличная часть ожидается в типовом вертикальном текстовом порядке после RTF-декодирования
- Если структура заголовков изменится, сервис вернет ошибку распознавания
