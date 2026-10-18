import json
import sqlite3
from datetime import datetime, timedelta
from kafka import KafkaConsumer

TOPICO = "transacoes"
BANCO = "fraudes.db"

def criar_banco():
    conexao = sqlite3.connect(BANCO)
    conexao.execute("""
        CREATE TABLE IF NOT EXISTS transacoes (
            transaction_id TEXT PRIMARY KEY,
            client_id      INTEGER,
            amount         REAL,
            city           TEXT,
            timestamp      TEXT,
            status         TEXT
        )
    """)
    conexao.commit()
    return conexao

def verificar_fraudes(conexao, t):
    fraudes = []
    momento = datetime.fromisoformat(t["timestamp"])
    if t["amount"] >= 10000:
        fraudes.append("ALTO_VALOR")
        
    inicio = (momento - timedelta(seconds=60)).isoformat(timespec="seconds")
    anteriores = conexao.execute(
        "SELECT COUNT(*) FROM transacoes WHERE client_id = ? AND timestamp > ?",
        (t["client_id"], inicio),
    ).fetchone()[0]
    if anteriores + 1 >= 4:
        fraudes.append("TEMPO_60s")
    
    inicio = (momento - timedelta(minutes=10)).isoformat(timespec="seconds")
    outra_cidade = conexao.execute(
        "SELECT COUNT(*) FROM transacoes WHERE client_id = ? AND timestamp >= ? AND city <> ?",
        (t["client_id"], inicio, t["city"]),
    ).fetchone()[0]
    if outra_cidade > 0:
        fraudes.append("GEO_10m")
    return fraudes

def salvar(conexao, t, status):
    conexao.execute(
        "INSERT OR IGNORE INTO transacoes VALUES (?, ?, ?, ?, ?, ?)",
        (t["transaction_id"], t["client_id"], t["amount"], t["city"], t["timestamp"], status),
    )
    conexao.commit()

def main():
    conexao = criar_banco()
    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers="localhost:9092",
        group_id="detector-de-fraude",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print("Consumer iniciado. Aguardando transações... (Ctrl+C para parar)")

    for mensagem in consumer:
        t = mensagem.value
        fraudes = verificar_fraudes(conexao, t)
        status = ", ".join(fraudes) if fraudes else "OK"
        salvar(conexao, t, status)

        detalhes = (f"Cliente {t['client_id']} | R$ {t['amount']:.2f} | "
                    f"{t['city']} | {t['timestamp']} | id {t['transaction_id'][:8]}")
        if fraudes:
            print(f"*** ALERTA DE FRAUDE [{status}] *** {detalhes}")
        else:
            print(f"[OK] {detalhes}")

if __name__ == "__main__":
    main()