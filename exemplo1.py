import requests as rq
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime # para os esquemas do banco 
from sqlalchemy.orm import declarative_base, sessionmaker # para os insert no banco 
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import os
load_dotenv()
# Configurações do banco de dados
DATABASE_URL = "postgresql://dbnamr_user:WfpyUcxzRoRoJCpE6QnFoa9p9ckTQ7bD@dpg-d0ucl5re5dus7392us5g-a.oregon-postgres.render.com/dbnamr"

#DATABASE_URL = os.getenv("postgresql://dbnamr_user:WfpyUcxzRoRoJCpE6QnFoa9p9ckTQ7bD@dpg-d0ucl5re5dus7392us5g-a.oregon-postgres.render.com/dbnamr")
#DATABASE_URL = os.getenv("postgresql://meu_usuario:minhasenha@localhost/meubanco")

#sessção 
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()
#Base.metadata.create_all(engine)


# meu Database 
class BitcoinDados(Base):
    __tablename__ = "dados_do_bitcoin"
    
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

def extrair():
# API é uma url/site que so tem dados 
    url = "https://api.coinbase.com/v2/prices/spot"
    resposta = rq.get(url)
    return(resposta.json())

def transformar(dados_json):
    valor = float(dados_json['data']['amount'])
    cript = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    dados_tratados = BitcoinDados(
        valor=valor,
        criptomoeda=cript,
        moeda=moeda,
        timestamp=datetime.now()
    )
    return dados_tratados

def salvar(dados):

    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos!")
  
if __name__ == "__main__":

    while(True):
        dados_json = extrair()
        dados_tratados = transformar(dados_json)
        print("Dados Tratados:")        
        salvar(dados_tratados)
        # Pausar
        print("Aguardando 15 segundos...")
        sleep(15)
        