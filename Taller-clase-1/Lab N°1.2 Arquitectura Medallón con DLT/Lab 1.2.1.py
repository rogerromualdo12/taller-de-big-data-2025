# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 0
# MAGIC Error 429

# COMMAND ----------

import requests
import os
import json

# COMMAND ----------

# Función para obtener datos de CoinGecko API
def fetch_crypto_prices(crypto_ids, vs_currency="usd"):
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(crypto_ids),  # Lista de criptomonedas separada por comas
        "vs_currencies": vs_currency
    }
    response = requests.get(url, params=params)
    
    # Manejar errores
    if response.status_code != 200:
        raise Exception(f"Error en la API: {response.status_code}, {response.text}")
    
    return response.json()

# Probar la función
crypto_ids = ["bitcoin", "ethereum", "dogecoin"]
prices = fetch_crypto_prices(crypto_ids)
print(prices)

# COMMAND ----------

def stream_crypto_prices():
    while True:
        prices = fetch_crypto_prices(crypto_ids)
        timestamp = int(time.time())
        for crypto, data in prices.items():
            yield {
                "timestamp": timestamp,
                "crypto": crypto,
                "price_usd": data["usd"]
            }
        time.sleep(5)  # Esperar 5 segundos entre solicitudes

# COMMAND ----------

output_dir = "/FileStore/test_lab_1"
os.makedirs(output_dir, exist_ok=True)

for record in stream_crypto_prices():
    with open(f"{output_dir}{record['timestamp']}.json", "w") as f:
        f.write(json.dumps(record))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear un archivo de datos simulados
# MAGIC Debido a los límites de la API (429), podemos usar un generador de datos simulados que escriba eventos en una carpeta

# COMMAND ----------

import time
import json
import random
import os

output_dir = "/dbfs/tmp/crypto_dlt/"
os.makedirs(output_dir, exist_ok=True)

cryptos = ["bitcoin", "ethereum", "ripple", "cardano"]

while True:
    data = [
        {
            "timestamp": int(time.time()),
            "crypto": crypto,
            "price_usd": round(random.uniform(1000, 50000), 2)
        }
        for crypto in cryptos
    ]
    for record in data:
        with open(f"{output_dir}{record['timestamp']}_{record['crypto']}.json", "w") as f:
            f.write(json.dumps(record))
    time.sleep(10)

