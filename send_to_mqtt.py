import random
from paho.mqtt import client as mqtt_client

# Настройки подключения
broker = '127.0.0.1'  # Адрес брокера
port = 1883  # Порт для подключения
topic = "/devices/unit180/controls/реле4/on"  # Топик, в который отправляем сообщение
message_to_send = '{"value": 0}'  # Сообщение для отправки {value: 1}

# Генерируем случайный ID клиента
client_id = f'python-mqtt-{random.randint(0, 1000)}'


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Успешно подключились к MQTT брокеру!")
        else:
            print(f"Ошибка подключения, код: {rc}")

    # Создаем клиента
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    # Публикуем сообщение
    result = client.publish(topic, message_to_send)

    # Проверяем результат отправки
    status = result[0]
    if status == 0:
        print(f"Отправлено сообщение '{message_to_send}' в топик '{topic}'")
    else:
        print(f"Ошибка отправки сообщения в топик {topic}")


def run():
    client = connect_mqtt()
    client.loop_start()  # Запускаем фоновый цикл обработки сообщений
    publish(client)
    client.loop_stop()  # Останавливаем цикл после отправки


if __name__ == '__main__':
    run()