import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

TOPICO = "transacoes"

CLIENTES = {
    1: "Fortaleza",
    2: "Recife",
    3: "São Paulo",
    4: "Salvador",
    5: "Belo Horizonte",
}
CIDADES = list(CLIENTES.values())

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


def gerar_transacao():
    client_id = random.choice(list(CLIENTES))

    if random.random() < 0.8:
        city = CLIENTES[client_id]
    else:
        city = random.choice(CIDADES)
        
    if random.random() < 0.1:
        amount = round(random.uniform(10000, 30000), 2)
    else:
        amount = round(random.uniform(10, 5000), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "client_id": client_id,
        "amount": amount,
        "city": city,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

print("Iniciado. Enviando uma transação a cada 3 segundos...")

while True:
    transacao = gerar_transacao()
    producer.send(TOPICO, transacao)
    producer.flush()
    print(f"Enviada: {transacao}")
    time.sleep(3)