import logging

import snowflake.connector
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.models.blocks import SectionBlock, ActionsBlock, ButtonElement

# 슬랙 채널 ID 설정
TARGET_CHANNEL_ID = "C082FEQMW0K"

app = App(token=Variable.get("SLACK_BOT_TOKEN"))
handler = SocketModeHandler(app, Variable.get("SLACK_APP_TOKEN"))

# 사용자 상태 저장
user_states = {}

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_snowflake():
    try:
        conn = BaseHook.get_connection("snowflake_conn_id")
        snowflake_config = {
            "user": conn.login,
            "password": conn.password,
            "account": conn.extra_dejson.get("account"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "database": conn.schema,
            "schema": conn.extra_dejson.get("schema"),
        }
        return snowflake.connector.connect(**snowflake_config)
    except Exception as e:
        logger.error(f"Snowflake 연결 오류: {e}")
        raise


def reset_user_state(user_id):
    if user_id in user_states:
        del user_states[user_id]


def display_initial_options(say):
    blocks = [
        SectionBlock(block_id="section1", text="어떤 정보를 확인하시겠습니까?"),
        ActionsBlock(
            block_id="actions1",
            elements=[
                ButtonElement(text="최근 주요 국가 환율", action_id="button_daily_rate", value="daily_rate"),
                ButtonElement(text="S&P 500 지수", action_id="button_sp500", value="sp500"),
                ButtonElement(text="원유 가격 (브렌트 유)", action_id="button_crude_oil", value="crude_oil"),
            ],
        ),
    ]
    say(blocks=blocks)


def get_historical_rates():
    conn = connect_to_snowflake()
    try:
        with conn.cursor() as cur:
            query = """
            WITH daily_change AS (
                SELECT 
                    DATE,
                    CURRENCY,
                    CLOSE,
                    LAG(CLOSE) OVER (PARTITION BY CURRENCY ORDER BY DATE) as prev_close,
                    ((CLOSE - LAG(CLOSE) OVER (PARTITION BY CURRENCY ORDER BY DATE)) / 
                     LAG(CLOSE) OVER (PARTITION BY CURRENCY ORDER BY DATE) * 100) as change_rate
                FROM DEV.RAW_DATA.CURRENCY
                ORDER BY DATE DESC
            )
            SELECT DATE, CURRENCY, CLOSE, change_rate
            FROM daily_change
            WHERE change_rate IS NOT NULL
            LIMIT 4;
            """
            cur.execute(query)
            results = cur.fetchall()
            return results
    except Exception as e:
        logger.error(f"Historical 환율 조회 오류: {e}")
        return None
    finally:
        conn.close()


def get_sp500_data():
    conn = connect_to_snowflake()
    try:
        with conn.cursor() as cur:
            query = """
            WITH daily_change AS (
                SELECT 
                    DATE,
                    INDEX_VALUE,
                    LAG(INDEX_VALUE) OVER (ORDER BY DATE) as prev_value,
                    ((INDEX_VALUE - LAG(INDEX_VALUE) OVER (ORDER BY DATE)) / 
                     LAG(INDEX_VALUE) OVER (ORDER BY DATE) * 100) as change_rate
                FROM DEV.RAW_DATA.SP500
                ORDER BY DATE DESC
            )
            SELECT DATE, INDEX_VALUE, change_rate
            FROM daily_change
            WHERE change_rate IS NOT NULL
            LIMIT 7;
            """
            cur.execute(query)
            results = cur.fetchall()
            return results
    except Exception as e:
        logger.error(f"S&P 500 데이터 조회 오류: {e}")
        return None
    finally:
        conn.close()


def get_crude_oil_data():
    conn = connect_to_snowflake()
    try:
        with conn.cursor() as cur:
            query = """
            WITH daily_change AS (
                SELECT 
                    DATE,
                    CLOSE,
                    LAG(CLOSE) OVER (ORDER BY DATE) as prev_close,
                    ((CLOSE - LAG(CLOSE) OVER (ORDER BY DATE)) / 
                     LAG(CLOSE) OVER (ORDER BY DATE) * 100) as change_rate
                FROM DEV.RAW_DATA.BRENT_OIL_PRICES
                ORDER BY DATE DESC
            )
            SELECT DATE, CLOSE, change_rate
            FROM daily_change
            WHERE change_rate IS NOT NULL
            LIMIT 7;
            """
            cur.execute(query)
            results = cur.fetchall()
            return results
    except Exception as e:
        logger.error(f"원유 가격 데이터 조회 오류: {e}")
        return None
    finally:
        conn.close()


def handle_event(event, say, logger):
    user_id = event["user"]
    channel_id = event["channel"]

    if channel_id != TARGET_CHANNEL_ID:
        return

    if user_id not in user_states and event["type"] == "app_mention":
        display_initial_options(say)
        user_states[user_id] = "waiting_for_selection"


@app.event("app_mention")
def handle_app_mention_events(body, say, logger):
    handle_event(body["event"], say, logger)


@app.action("button_daily_rate")
def handle_daily_rate(ack, body, say):
    ack()
    rates = get_historical_rates()
    if rates:
        message = "오늘의 환율 추이:\n"
        for rate in rates:
            # rate[0]은 DATE, rate[1]은 CURRENCY, rate[2]는 CLOSE, rate[3]은 change_rate
            change_symbol = "▲" if rate[3] > 0 else "▼"
            message += (
                f"{rate[0].strftime('%Y-%m-%d')} - {rate[1]}: {rate[2]:.2f} ({change_symbol}{abs(rate[3]):.2f}%)\n"
            )
        say(message)
    else:
        say("일간 환율 정보를 가져오는데 실패했습니다.")


@app.action("button_sp500")
def handle_sp500(ack, body, say):
    ack()
    sp500_data = get_sp500_data()
    if sp500_data:
        message = "S&P 500 지수 추이:\n"
        for data in sp500_data:
            # data[0]은 DATE, data[1]은 INDEX_VALUE, data[2]는 change_rate
            change_symbol = "▲" if data[2] > 0 else "▼"
            message += f"{data[0].strftime('%Y-%m-%d')}: {data[1]:,.2f} ({change_symbol}{abs(data[2]):.2f}%)\n"
        say(message)
    else:
        say("S&P 500 데이터를 가져오는데 실패했습니다.")


@app.action("button_crude_oil")
def handle_crude_oil(ack, body, say):
    ack()
    crude_oil_data = get_crude_oil_data()
    if crude_oil_data:
        message = "원유 가격 (브렌트 유) 추이:\n"
        for data in crude_oil_data:
            # data[0]은 DATE, data[1]은 CLOSE, data[2]는 change_rate
            change_symbol = "▲" if data[2] > 0 else "▼"
            message += f"{data[0].strftime('%Y-%m-%d')}: {data[1]:,.2f} ({change_symbol}{abs(data[2]):.2f}%)\n"
        say(message)
    else:
        say("원유 가격 데이터를 가져오는데 실패했습니다.")


if __name__ == "__main__":
    handler.start()
