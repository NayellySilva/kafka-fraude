import sqlite3

conexao = sqlite3.connect("fraudes.db")

total = conexao.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0]
fraudes = conexao.execute("SELECT COUNT(*) FROM transacoes WHERE status <> 'OK'").fetchone()[0]
print(f"Total de transações: {total}")
print(f"Total de fraudes: {fraudes}")

print("\nFraudes por tipo:")
for tipo in ["ALTO_VALOR", "TEMPO_60s", "GEO_10m"]:
    count = conexao.execute(
        "SELECT COUNT(*) FROM transacoes WHERE status = ?", (tipo,)
    ).fetchone()[0]
    print(f"{tipo}: {count}")

print("\nUltimas 10 transações:")
for linha in conexao.execute(
    "SELECT * FROM transacoes ORDER BY timestamp DESC LIMIT 10"
):
    print(linha)