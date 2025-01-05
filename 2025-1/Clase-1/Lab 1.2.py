# Databricks notebook source
import requests
import json
import os
import time
import random

# COMMAND ----------

def fetch_crypto_prices(crypto_ids, vs_currency="usd"):
  url = "https://api.coingecko.com/api/v3/simple/price"
  params = {
      "ids": ",".join(crypto_ids),
      "vs_currencies": vs_currency
  }
  response = requests.get(url, params=params)

  # Manejo de errores
  if response.status_code != 200:
      raise Exception(f"Error en la API: {response.status_code}, {response.text}")
  return response.json()

# COMMAND ----------

crypto_ids = ["bitcoin", "ethereum", "dogecoin", "cardano", "solana"]
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
        time.sleep(5) # Esperar 5 segundos entre solicitudes

# COMMAND ----------

output_dir = "/tmp/crypto_prices"
os.makedirs(output_dir, exist_ok=True)

for record in stream_crypto_prices():
    with open(f"{output_dir}/{record['timestamp']}.json", "w") as f:
        f.write(json.dumps(record))

# COMMAND ----------

# MAGIC %md
# MAGIC _Surge la necesidad de trabajar con datos sintéticos, por el límite de velocidad de retrieval de la API de Coin Gecko_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Crear un archivo de datos simulados
# MAGIC
# MAGIC Debido a los límite de la API(429), podemos un generador de datos simulados que escriba eventos en una carpeta

# COMMAND ----------

output_dir = "/dbfs/tmp/crypto_dlt"
os.makedirs(output_dir, exist_ok=True)

cryptos = ["bitcoin", "ethereum", "dogecoin", "cardano", "solana"]

while True:
    data = [
        {
            "timestamp": int(time.time()),
            "crypto": crypto,
            "price_usd": round(random.uniform(1, 100000), 2)
        }
        for crypto in cryptos
    ]
    for record in data:
        with open(f"{output_dir}{record['timestamp']}_{record['crypto']}.json", "w") as f:
            f.write(json.dumps(record))
    time.sleep(1)
