import json
import time
import random
import datetime
import paho.mqtt.client as mqtt

BROKER_HOST = "broker.hivemq.com"   
BROKER_PORT = 1883
PUBLISH_INTERVAL = 2                
DEVICE_ID = "factory_line_A"        

# MQTT Topic ：factory/{device_id}/{sensor_type}
TOPIC_TEMP     = f"factory/{DEVICE_ID}/temperature"
TOPIC_HUMIDITY = f"factory/{DEVICE_ID}/humidity"
TOPIC_PRESSURE = f"factory/{DEVICE_ID}/pressure"



def simulate_sensor_data() -> dict:
    """模擬感測器讀值（加入隨機飄移，模擬真實感測器行為）"""
    return {
        "device_id": DEVICE_ID,
        "timestamp": datetime.datetime.now().isoformat(),
        "temperature": round(random.uniform(22.0, 85.0), 2),   # °C
        "humidity":    round(random.uniform(30.0, 80.0), 2),   # %RH
        "pressure":    round(random.uniform(0.8, 1.2), 4),     # MPa
        "status": random.choices(
            ["normal", "warning", "error"],
            weights=[70, 15, 5]
        )[0]
    }


def on_connect(client, userdata, flags, rc, properties=None):
    status = {0: "連線成功", 1: "版本錯誤", 2: "識別碼錯誤",
              3: "Broker 無法使用", 4: "帳密錯誤", 5: "未授權"}
    print(f"[Broker] {status.get(rc, f'未知狀態碼 {rc}')}")


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    print(f"  ✓ 訊息已送達 (mid={mid})")


def main():
    print("*" * 50)
    print("  iot sensor publisher 啟動")
    print(f"  Broker : {BROKER_HOST}:{BROKER_PORT}")
    print(f"  設備ID : {DEVICE_ID}")
    print(f"  頻率   : 每 {PUBLISH_INTERVAL} 秒")
    print("*" * 50)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_publish = on_publish

    print(f"\n正在連線到 {BROKER_HOST}...")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    count = 0
    try:
        while True:
            count += 1
            data = simulate_sensor_data()

            client.publish(TOPIC_TEMP,     json.dumps({"value": data["temperature"], "unit": "°C",  "status": data["status"], "ts": data["timestamp"]}), qos=1)
            client.publish(TOPIC_HUMIDITY, json.dumps({"value": data["humidity"],    "unit": "%RH", "status": data["status"], "ts": data["timestamp"]}), qos=1)
            client.publish(TOPIC_PRESSURE, json.dumps({"value": data["pressure"],    "unit": "MPa", "status": data["status"], "ts": data["timestamp"]}), qos=1)

            client.publish(
                f"factory/{DEVICE_ID}/all",
                json.dumps(data),
                qos=1
            )

            status_icon = {"normal": "🟢", "warning": "🟡", "error": "🔴"}.get(data["status"], "⚪")
            print(f"\n[#{count}] {data['timestamp']}")
            print(f"  溫度: {data['temperature']}°C  |  濕度: {data['humidity']}%  |  壓力: {data['pressure']} MPa  {status_icon} {data['status']}")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n publisher has stopped（Ctrl+C）")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
