## Краткое назначение

`gamalytic.py` — скрипт для асинхронного сбора данных через Gamalytic API и расчёта статистик (revenue, copiesSold, price и т.д.).
По умолчанию: *fetch → save → stats → write results.txt*. Есть режимы только fetch (сбор данных) и только stats (расчёт статистики).

Для получения api ключа:
* Зайти на https://api.gamalytic.com/reference/
* Кликнуть по Authorize
* Придумать и ввести api ключ (например 12121212)
* Нажать Authorize, окно не закрывать

---

## Запуск и примеры

```bash
# Стандартный запуск (fetch -> save -> stats), F2P исключены:
python3 gamalytic.py --appids appids.txt --out-data gamalytic.csv --api-key YOUR_KEY

# Включить F2P (price == 0) в сохранение и расчёт:
python3 gamalytic.py --appids appids.txt --out-data gamalytic.csv --add-f2p --api-key YOUR_KEY

# Только собрать данные и сохранить (без расчётов):
python3 gamalytic.py --appids appids.txt --out-data gamalytic.json --format json --fetch-only --api-key YOUR_KEY

# Только расчёты по существующему файлу (без запросов):
python3 gamalytic.py --stats-only --out-data gamalytic.csv

# С rate-limiter и умеренной скоростью:
python3 gamalytic.py --appids appids.txt --out-data gamalytic.csv --concurrency 3 --delay 0.5 --rps 1.0 --api-key YOUR_KEY
```

---

## Флаги — полный список

> Формат: **`--flag`** `(тип)` — описание. **(по умолчанию: значение)**

### Основные вход/выход

* **`--appids <path>`** `(string)` — файл со списком Steam `appid`, по одному в строке.
  **(обязателен**, если не используется `--stats-only`)
* **`--api-key <string>`** — Gamalytic API key. Если не указан, берётся из переменной окружения `GAMALYTIC_API_KEY`.
  **(по умолчанию: не задано — будет предупреждение)**
* **`--out-data <path>`** `(string)` — куда сохранять сброшенные данные (CSV или JSON).
  **(по умолчанию: `gamalytic.csv`)**
* **`--format {csv|json}`** — формат сохранения данных. **(по умолчанию: `csv`)**
  Примечание: если `--out-data` имеет расширение `.json`, сохранение будет в JSON независимо от `--format`.
* **`--out-results <path>`** `(string)` — путь для итогового текстового отчёта с расчётами (UTF-8).
  **(по умолчанию: `results.txt`)**

### Режимы работы

* **`--fetch-only`** — только получить данные и сохранить (в `--out-data`). Не выполнять расчёты.
* **`--stats-only`** — только выполнить расчёты по существующему `--out-data`. Не обращаться к API.

  > `--fetch-only` и `--stats-only` — *взаимоисключающие*; нельзя указывать оба одновременно.

### Фильтрация F2P

* **`--add-f2p`** — включать в сохранение и расчёты игры с `price == 0` (F2P).
  **(по умолчанию: *выключен* — F2P исключаются)**

### Параллелизм / таймаут / задержки

* **`--concurrency, -c <int>`** — число параллельных задач (семафор). **(по умолчанию: `5`)**
* **`--timeout <int>`** — таймаут на HTTP-запрос в секундах. **(по умолчанию: `30`)**
* **`--delay <float>`** — фиксированная задержка (сек) *перед* выполнением каждого запроса внутри задачи. **(по умолчанию: `0.0`)**

### Rate-limiter (Adaptive Token Bucket)

* **`--rps <float>`** — стартовая скорость (requests per second). **(по умолчанию: `1.0`)**
* **`--rps-min <float>`** — минимальная rps после снижения. **(по умолчанию: `0.1`)**
* **`--rps-max <float>`** — максимальная rps. **(по умолчанию: `5.0`)**
* **`--rps-increase <float>`** — additive step увеличения rps при успехах. **(по умолчанию: `0.05`)**
* **`--rps-decrease-factor <float>`** — multiplicative factor при 429 (умножается на текущую rps). **(по умолчанию: `0.5`)**

### Вывод / подсчёты

* **`--top <int>`** — сколько записей показывать в секции Top N (по revenue). **(по умолчанию: `5`)**

---

## Поведение по умолчанию и важные замечания

* **F2P (price == 0)**: по умолчанию такие игры **исключаются** из выходного файла и из расчётов. Чтобы включить их — используйте `--add-f2p`.
* **Режимы**:

  * Без флагов: выполняется `fetch → save (filtered) → stats → save results`.
  * `--fetch-only`: fetch → save → exit (без stats).
  * `--stats-only`: читается `--out-data` → stats → save results (не требуется `--appids`).
* **Лимиты**: скрипт обрабатывает `HTTP 429` и `Retry-After`. При 429 токен-бакет уменьшит rps и установит «охлаждение».
* **Delay vs RPS**:

  * `--delay` — фиксированная небольшая пауза в каждой задаче.
  * Token bucket (`--rps`) — глобальное ограничение throughput; обе механики работают вместе.
* **CSV vs JSON**: `--format` управляет форматом, но расширение `--out-data` имеет приоритет (если `.json` — сохранится JSON).

---

## Рекомендованные конфигурации (эмпирически)

* **Безопасный старт (free API)**
  `--concurrency 3 --rps 1.0 --delay 1.0`
  Умеренный параллелизм, небольшой delay — снижает шанс коротких всплесков и 429.
* **Быстрый (с риском 429)**
  `--concurrency 10 --rps 5.0 --delay 0.0`
* **Экономия квоты (растянуть во времени)**
  `--concurrency 1 --rps 0.2 --delay 2.0`

---

## Советы по практическому применению

* Дневная квота для бесплатного API - 500 запросов.
* Если получаете много `429`, уменьшите `--rps` или `--concurrency`, либо увеличьте `--delay`.

---

## Таблица флагов

```md
# Флаги gamalytic.py (кратко)
--appids <path>        (string)  — файл с appid (одна строка = один appid)
--api-key <string>     (string)  — API key (или из GAMALYTIC_API_KEY)
--out-data <path>      (string)  — куда сохранить данные (csv/json). (default: gamalytic.csv)
--format {csv|json}    — формат сохранения (default: csv)
--out-results <path>   — куда сохранить итоговый текстовый отчёт (default: results.txt)
--fetch-only           — только получить данные и сохранить (без расчётов)
--stats-only           — только расчёты по существующему файлу (без fetch)
--add-f2p              — включать игры с price==0 в сохранение и расчёты
--concurrency, -c <n>  — параллелизм (default: 5)
--timeout <sec>        — таймаут запроса (default: 30)
--delay <sec>          — задержка **перед** каждым запросом (default: 0.0)
--rps <float>          — стартовая requests-per-second (default: 1.0)
--rps-min <float>      — минимальная rps (default: 0.1)
--rps-max <float>      — максимальная rps (default: 5.0)
--rps-increase <float> — add step on success (default: 0.05)
--rps-decrease-factor  — multiplicative decrease on 429 (default: 0.5)
--top <n>              — сколько результатов в Top games N (default: 5)
```
