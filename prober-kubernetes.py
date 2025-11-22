import time
import os
import logging
import requests
import pymysql
from datetime import datetime, timedelta
from prometheus_client import start_http_server, Counter, Histogram
from apscheduler.schedulers.background import BackgroundScheduler

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
NAMESPACE = os.getenv("NAMESPACE", "st-ab4-baranov")
PERIOD = os.getenv("PERIOD", "2m")
SAGE_URL = os.getenv("SAGE_URL", "https://sage.sre-ab.ru/mage/api/search")
SAGE_TOKEN = os.getenv("SAGE_TOKEN",
                                "eyJraWQiOiI2REFFREQ5Q0M5RUIxMDcyQUVDQTE4Qzg1RjMwNERFRDdGMEEyNDkxRERDRTYyNDk5RjlDRTkzRjlEOEJEODI1IiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJpc3MiOiJtYW51bCIsImV4cCI6NDEwMjQ0NDgwMCwiaWF0IjoxNzYyOTM1NzgwLCJncm91cCI6WyJzYWdlX2FiNF9iYXJhbm92Il0sInVwbiI6ImFiNF9iYXJhbm92QGtleWNsb2FrLmxvY2FsaG9zdCIsInBlcm1pc3Npb25zIjpbXSwiY2xpZW50SWQiOiJzYWdlX3Rva2VuIiwianRpIjoiN2JmYjI0NzgtYmM3MC00MzA1LWJmMmEtNzhkOTg3OWYzZmI3In0.LpVXsN_JdOOOWTWHFc9NeWIVPKqUPkeoRaum8DyW7ujHlMUBowucb5Ma-B5v9fJlnNtPk6_jLKjLWpxyElHWZ2_rbQpl1cMT0_xdGHFiAbU6SU48yeyGd2S8ubqBWrpqZDsp-7abxjN13em_9bH30sZdujY1EEZbLpbn7mVLq-Qe16pQzjMOfR4fb7n9T_36Mp1DyldB2vbYp_CkU8hqoB_yRUgRYVnbtaQm6LmpxjzWk5T252WG0bu-9jDJgpsquK4VDgaKeYhGm-9IBHym4Sy00RgYNOKaGL_xHpFXQ7dzceZ9xEGeo-xUDgA-ABBD8C87ZB8TWfj9pVENrHm46A")
SOURCE_HEADER = os.getenv("SOURCE_HEADER", "python-prober")
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "oncall")


# Метрики Prometheus для мониторинга длительности и количества HTTP-запросов пробера
PROBER_DURATION = Histogram(
    "prober_http_request_duration_seconds",
    "Длительность HTTP-запросов пробера",
    ["namespace", "scenario", "endpoint"]
)
PROBER_TOTAL = Counter(
    "prober_http_requests_total",
    "Всего HTTP-запросов пробера",
    ["namespace", "scenario", "endpoint", "code"]
)

# Функция для установления соединения с базой данных
def get_db_connection():
    try:
        # Создание соединения с использованием параметров из переменных окружения
        conn = pymysql.connect(host=DB_HOST,
                               user=DB_USER,
                               password=DB_PASS,
                               database=DB_NAME,
                               autocommit=True,
                               cursorclass=pymysql.cursors.DictCursor)
        return conn
    except Exception as e:
        logging.error("DB connect error: %s", e)
        return None

# Функция для выполнения пробного HTTP-запроса с измерением метрик
# Замеряет время, обновляет метрики, обрабатывает исключения
def do_probe(method, url, scenario, endpoint, **kwargs):
    start = time.time()
    code = "exception"
    try:
        # Выполнение HTTP-запроса с таймаутом 10 секунд
        r = requests.request(method, url, timeout=10, **kwargs)
        code = str(r.status_code)
        duration = time.time() - start
        # Обновление гистограммы длительности с метками
        PROBER_DURATION.labels(namespace=NAMESPACE, scenario=scenario, endpoint=endpoint).observe(duration)
        # Инкремент счетчика запросов с метками, включая код
        PROBER_TOTAL.labels(namespace=NAMESPACE, scenario=scenario, endpoint=endpoint, code=code).inc()
        r.raise_for_status()
        return r
    except Exception as e:
        duration = time.time() - start
        # Обновление метрик для ошибочного случая
        PROBER_DURATION.labels(namespace=NAMESPACE, scenario=scenario, endpoint=endpoint).observe(duration)
        PROBER_TOTAL.labels(namespace=NAMESPACE, scenario=scenario, endpoint=endpoint, code=code).inc()
        logging.warning("%s %s failed: %s", method, url, e)
        raise

# Функция для пробирования эндпоинта healthcheck
# Использует do_probe для GET-запроса
def probe_health():
    do_probe("GET", f"{BASE_URL}/healthcheck", scenario="healthcheck", endpoint="/healthcheck")

# Проба для проверки получения списка команд
def probe_teams():
    do_probe("GET", f"{BASE_URL}/api/v0/teams", scenario="get_teams", endpoint="/api/v0/teams")

# Проба для цикла создания и удаления roster
def probe_roster():
    team = "Test Team"
    roster = "test-roster"
    cleanup_url = f"{BASE_URL}/api/v0/teams/{team}/rosters/{roster}"
    create_url = f"{BASE_URL}/api/v0/teams/{team}/rosters"
    try:
        # Попытка удаления roster перед созданием (игнорирование ошибок)
        do_probe("DELETE", cleanup_url, scenario="roster_cycle", endpoint=f"/api/v0/teams/{team}/rosters/{roster}")
    except:
        pass
    # Создание roster с JSON-телом
    do_probe("POST", create_url, scenario="roster_cycle", endpoint=f"/api/v0/teams/{team}/rosters",
             json={"name": roster})
    try:
        # Повторная очистка после создания
        do_probe("DELETE", cleanup_url, scenario="roster_cycle", endpoint=f"/api/v0/teams/{team}/rosters/{roster}")
    except:
        pass

# Функция для выполнения PromQL-запроса к Sage API
# Формирует тело запроса с временным диапазоном
def sage_query(promql):
    now = datetime.utcnow().replace(second=0, microsecond=0)
    start = (now - timedelta(minutes=11)).isoformat() + "Z"
    end = now.isoformat() + "Z"
    body = {
        "query": f"pql {promql}",
        "size": 1,
        "startTime": start,
        "endTime": end
    }
    headers = {
        "SOURCE": SOURCE_HEADER,
        "Authorization": f"Bearer {SAGE_TOKEN}"
    }
    try:
        # POST-запрос к Sage API
        r = requests.post(SAGE_URL, json=body, headers=headers, timeout=10)
        r.raise_for_status()# Проверка на ошибки
        data = r.json()
        if data.get("hits"):
            return float(data["hits"][0]["value"])
    except Exception as e:
        logging.warning("Sage error: %s", e)
    return None

# Функция для расчета и сохранения только SLA индикаторов
def calculate_sla():
    conn = get_db_connection()
    if not conn:
        logging.error("No DB connection, skipping SLA calculation")
        return
    try:
        with conn.cursor() as cursor:

            # Расчет процента успешных запросов (SLA: 98%)
            success_rate_promql = (
                '100 * sum(increase(prober_http_requests_total{{namespace="{}",code=~"2.."}}[{period}])) '
                '/ sum(increase(prober_http_requests_total{{namespace="{}"}}[{period}]))'
            ).format(NAMESPACE, NAMESPACE, period=PERIOD)
            success_rate = sage_query(success_rate_promql) or 0.0

            # Расчет 90-го перцентиля задержки (SLA: < 0.15s)
            latency_90th_percentile_promql = (
                'histogram_quantile(0.90, sum(rate(prober_http_request_duration_seconds_bucket{{namespace="{}"}}[{period}])) by (le))'
            ).format(NAMESPACE, period=PERIOD)
            latency_90th_percentile = sage_query(latency_90th_percentile_promql) or 999

            # Сохранение только SLA индикаторов
            sql = "INSERT INTO indicators (name, slo, value, is_bad, datetime) VALUES (%s, %s, %s, %s, NOW())"
            records = [
                # SLA для успеха: порог 98%, is_bad=1 если ниже
                ("api_success_rate_sla_percent", 98.0, success_rate, 1 if success_rate < 98.0 else 0),
                # SLA для задержки: порог 0.15s, is_bad=1 если выше
                ("api_latency_90th_percentile_sla_seconds", 0.15, latency_90th_percentile, 1 if latency_90th_percentile > 0.15 else 0),
            ]
            for rec in records:
                try:
                    cursor.execute(sql, rec)
                    logging.info("Saved %s = %.4f (bad=%s)", rec[0], rec[2], rec[3])
                except Exception as e:
                    logging.error("DB save error %s: %s", rec[0], e)
    finally:
        conn.close()

if __name__ == "__main__":
    # Запуск эндпоинта для метрик Prometheus
    start_http_server(int(os.getenv("METRICS_PORT", 9091)))
    scheduler = BackgroundScheduler()
    # Добавление задач в планировщик с интервалами из переменных окружения
    scheduler.add_job(probe_health, "interval", seconds=int(os.getenv("PROBE_HEALTH_INTERVAL", 30)))
    scheduler.add_job(probe_teams, "interval", seconds=int(os.getenv("PROBE_TEAMS_INTERVAL", 60)))
    scheduler.add_job(probe_roster, "interval", seconds=int(os.getenv("PROBE_ROSTER_INTERVAL", 60)))
    scheduler.add_job(calculate_sla, "interval", seconds=int(os.getenv("CALCULATE_SLA_INTERVAL", 60)))  # 5 минут
    scheduler.start()
    # Логирование запуска пробера с портом метрик
    logging.info("Python prober запущен: :%s/metrics", os.getenv("METRICS_PORT", 9091))
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down")
        scheduler.shutdown()