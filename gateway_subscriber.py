import json
import time
import datetime
import statistics
from collections import defaultdict, deque
import paho.mqtt.client as mqtt

BROKER_HOST = "broker.hivemq.com"
BROKER_PORT = 1883
DEVICE_ID   = "factory_line_A"
LOG_FILE    = "gateway_log.txt"

SUBSCRIBE_TOPIC = f"factory/{DEVICE_ID}/#"

THRESHOLDS = {
    "temperature": {"min": 20.0,  "max": 80.0,  "unit": "°C"},
    "humidity":    {"min": 25.0,  "max": 75.0,  "unit": "%RH"},
    "pressure":    {"min": 0.85,  "max": 1.15,  "unit": "MPa"},
}

data_windows = defaultdict(lambda: deque(maxlen=20))
alert_count  = defaultdict(int)
msg_count    = 0


def write_log(message: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def check_threshold(sensor_type: str, value: float):
    if sensor_type not in THRESHOLDS:
        return None
    t = THRESHOLDS[sensor_type]
    if value < t["min"]:
        return f"⚠️  [{sensor_type}] 數值 {value}{t['unit']} 低於下限 {t['min']}{t['unit']}"
    if value > t["max"]:
        return f"🚨  [{sensor_type}] 數值 {value}{t['unit']} 超過上限 {t['max']}{t['unit']}"
    return None


def print_stats(sensor_type: str):
    """print sensor current data"""
    window = data_windows[sensor_type]
    if len(window) < 2:
        return
    unit = THRESHOLDS.get(sensor_type, {}).get("unit", "")
    avg  = round(statistics.mean(window), 2)
    mx   = round(max(window), 2)
    mn   = round(min(window), 2)
    std  = round(statistics.stdev(window), 3) if len(window) > 1 else 0
    print(f"     📊 統計({len(window)}筆) 平均:{avg}{unit}  最大:{mx}{unit}  最小:{mn}{unit}  標準差:{std}")


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[Broker] 連線成功")
        client.subscribe(SUBSCRIBE_TOPIC, qos=1)
        print(f"[Gateway] 已訂閱 Topic: {SUBSCRIBE_TOPIC}\n")
    else:
        print(f"[Broker] 連線失敗，狀態碼: {rc}")


def on_message(client, userdata, msg):
    global msg_count
    msg_count += 1

    topic   = msg.topic
    payload = msg.payload.decode("utf-8")

    # 解析 topic 取得感測器類型
    # format：factory/{device_id}/{sensor_type}
    parts       = topic.split("/")
    sensor_type = parts[-1] if len(parts) >= 3 else "unknown"

    # 忽略彙整訊息（all），只處理單一感測器
    if sensor_type == "all":
        return

    try:
        data  = json.loads(payload)
        value = data.get("value")
        unit  = data.get("unit", "")
        ts    = data.get("ts", "")

        print(f"[#{msg_count}] Topic: {topic}")
        print(f"     數值: {value} {unit}  |  時間: {ts}")

        # 寫入滑動視窗
        if isinstance(value, (int, float)):
            data_windows[sensor_type].append(value)

            # 閾值檢查
            alert = check_threshold(sensor_type, value)
            if alert:
                alert_count[sensor_type] += 1
                print(f"     {alert}")
                write_log(f"ALERT | {sensor_type} | {value}{unit} | {alert}")
            else:
                write_log(f"DATA  | {sensor_type} | {value}{unit} | OK")

            # 每 5 筆印一次統計
            if len(data_windows[sensor_type]) % 5 == 0:
                print_stats(sensor_type)

    except json.JSONDecodeError:
        print(f"[錯誤] 無法解析 JSON: {payload}")


def on_disconnect(client, userdata, rc, properties=None, reasonCode=None):
    if rc != 0:
        print(f"[Gateway] 非預期斷線 (rc={rc})，嘗試重連...")


def main():
    print("=" * 55)
    print("  IoT Gateway 訂閱者啟動")
    print(f"  Broker : {BROKER_HOST}:{BROKER_PORT}")
    print(f"  監控   : {SUBSCRIBE_TOPIC}")
    print(f"  Log    : {LOG_FILE}")
    print("=" * 55 + "\n")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    client.reconnect_delay_set(min_delay=1, max_delay=30)

    print(f"正在連線到 {BROKER_HOST}...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

    try:
        client.loop_forever()   # block waiting and process reconnect
    except KeyboardInterrupt:
        print(f"\n\nGateway 已停止（Ctrl+C）")
        print(f"共接收 {msg_count} 則訊息")
        for s, cnt in alert_count.items():
            print(f"  {s}: {cnt} 次警告")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
