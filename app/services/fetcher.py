import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional
from app.config.logger import setup_logger

logger = setup_logger("logs/fetcher.log")

def fetch_and_save(path:str,
                   symbol: str,
                   api_url: str,
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None,
                   interval: str = "1m",
                   period: Optional[str] = "1d",
                   auto_adjust: bool = True,):

    logger.info(f"[fetch_and_save] Símbolo={symbol}, de {start_date} até {end_date}, intervalo:({interval}), período:({period}), auto_adjust:({auto_adjust})")

    timestamp = datetime.now().isoformat()
    evo_path = f"{path}/{symbol}_evolution.csv"
    meta_path = f"{path}/{symbol}_metadata.csv"

    params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "period": period,
        "auto_adjust": str(auto_adjust),
    }
    resp = requests.get(api_url, params=params)
    resp.raise_for_status()
    data = resp.json()

    #salvar dados históricos
    #impedir que o ultimo dado entre de forma repetida
    evo = pd.DataFrame(data["data_evolution"])
    if not os.path.exists(evo_path):
        evo.to_csv(evo_path, index=False)
        logger.info(f"• Criando {symbol}_evolution.csv com {len(evo)} linhas.")
    else:
        last_timestamp: Optional[str] = None
        try:
            with open(evo_path, "rb") as f:
                f.seek(-256, os.SEEK_END)
                last_line = f.readlines()[-1].decode().strip()
                last_timestamp = last_line.split(",")[0]
        except (OSError, IndexError):
            last_timestamp = None
            logger.warning(f"Erro ao ler a última linha de {symbol}_evolution.csv.")
        
        if last_timestamp:
            evo["datetime_ts"] = pd.to_datetime(evo["datetime"], format="%Y-%m-%d %H:%M:%S")
            last_ts = pd.to_datetime(last_timestamp, format="%Y-%m-%d %H:%M:%S")
            novos = evo[evo["datetime_ts"] > last_ts].copy()
            novos = novos.drop(columns=["datetime_ts"])
        else:
            novos = evo
        
        if not novos.empty:
            # 2.3) Anexa apenas as novas linhas (sem cabeçalho)
            with open(evo_path, "a", newline="") as f_out:
                novos.to_csv(f_out, header=False, index=False)
            logger.info(f"• Foram adicionadas {len(novos)} novas linhas a {symbol}_evolution.csv")
        else:
            logger.info("• Nenhuma linha nova para adicionar (já existe um registro com mesmo timestamp).")


    # Salva metadados
    info = {k: data[k] for k in data if k != "data_evolution"}
    info_df = pd.DataFrame([{**{"timestamp": timestamp}, **info}])
    write_header = not os.path.isfile(meta_path)
    info_df.to_csv(meta_path, mode="a", header=write_header, index=False)
