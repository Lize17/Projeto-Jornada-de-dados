import requests
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import os
load_dotenv()

# Configurações padroes para iniciar o banco
#DATABASE_URL = ("postgresql://meu_usuario:minhasenha@localhost/meubanco")

DATABASE_URL = "postgresql://dbnamr_user:WfpyUcxzRoRoJCpE6QnFoa9p9ckTQ7bD@dpg-d0ucl5re5dus7392us5g-a.oregon-postgres.render.com/dbnamr"

# Cria o engine (motor de conexão) que define a URL do banco de dados e gerencia a comunicação com ele
engine = create_engine(DATABASE_URL)
# Cria uma fábrica de sessões, que são usadas para interagir com o banco (consultas, inserções, commits, etc.)
Session = sessionmaker(bind=engine)
# Cria a classe base para definir as tabelas do banco a partir dessa classe)
Base = declarative_base()

# Definição da tabela do banco 
class BitcoinDados(Base):
    __tablename__ = "dados_do_bitcoin"
    
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    cript = Column(String(10))
    moeda = Column(String(10))
    time = Column(DateTime)

# Cria a tabela (se não existir)
Base.metadata.create_all(engine)

def extrair():

    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    return resposta.json()

def transformar(dados_json):

    valor = float(dados_json['data']['amount'])
    criptomoeda = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    
    dados_tratados = BitcoinDados(
        valor=valor,
        cript=criptomoeda,
        moeda=moeda,
        time=datetime.now()
    )
    return dados_tratados

def salvar(dados):

    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos com sucesso!")

if __name__ == "__main__":
    while True:
       
        dados_json = extrair()
        dados_tratados = transformar(dados_json)

        salvar(dados_tratados)

        # Pausa
        print("Aguardando 15 segundos...")
        sleep(15)