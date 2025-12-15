# test_ca224_tcp.py
import logging
from pymodbus.client import ModbusTcpClient
import pymodbus

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("test")

HOST = "192.168.0.7"
PORT = 23          # если Modbus всё же на 502 — поменяешь на 502
SLAVE_ID = 224     # Modbus-адрес CA-224

def main():
    print(f"pymodbus version: {pymodbus.__version__}")

    client = ModbusTcpClient(
        host=HOST,
        port=PORT,
        timeout=2.0,
    )

    if not client.connect():
        print(f"❌ Не удалось подключиться к {HOST}:{PORT}")
        return

    try:
        print(f"Читаем coil 0, device_id={SLAVE_ID} ...")
        # КЛЮЧЕВОЕ: никакого unit/slave, только device_id=
        rr = client.read_coils(address=0, count=1, device_id=SLAVE_ID)

        if rr.isError():
            print(f"❌ Ошибка Modbus-ответа: {rr}")
        else:
            bits = getattr(rr, "bits", None)
            val = bits[0] if bits else None
            print(f"✅ Ответ: coil[0] = {val}, raw bits={bits}")

    except Exception as e:
        print(f"❌ Исключение при опросе: {e}")
    finally:
        client.close()
        print("Соединение закрыто.")

if __name__ == "__main__":
    main()
