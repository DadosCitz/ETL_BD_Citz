import pandas as pd
import requests
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client
import json
from datetime import datetime

# =======================================================
# Configuração do Supabase
# =======================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
API_TOKEN = os.environ["API_TOKEN"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
tabela = supabase.schema("Comercial_Citz").table("d_corretores")

# =======================================================
# Configuração da Sessão HTTP
# =======================================================
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[408, 429, 500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retry_strategy))

headers = {
    "accept": "application/json",
    "email": "thiago.almeida@citz.co",
    "token": API_TOKEN,
    "content-type": "application/json"
}

# =======================================================
# Funções Auxiliares (MODIFICADAS)
# =======================================================
def make_safe_request(url, payload, attempt=1, max_attempts=3):
    """Faz requisições HTTP com tratamento de erros e retry"""
    try:
        response = session.get(
            url,
            json=payload,
            headers=headers,
            timeout=(10, 30))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        if attempt < max_attempts:
            wait_time = 2 ** attempt
            print(f"Timeout na tentativa {attempt}. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            return make_safe_request(url, payload, attempt+1, max_attempts)
        raise
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {str(e)}")
        raise

def prepare_data(df):
    """Prepara os dados para o Supabase com tratamento especial para datas"""
    # Converter tudo para string e tratar nulos
    df = df.astype(str).fillna('')
    
    # Tratamento ESPECIAL para data_cad (convertendo para formato ISO com timezone)
    if 'data_cad' in df.columns:
        df['data_cad'] = pd.to_datetime(df['data_cad'], errors='coerce')
        # Remove timezone se existir e formata para ISO sem timezone
        df['data_cad'] = df['data_cad'].apply(
            lambda x: x.isoformat() if not pd.isna(x) else None
        )
    
    # Tratamento para outras colunas de data (se houver)
    other_date_cols = [col for col in df.columns if 'data' in col.lower() and col != 'data_cad']
    for col in other_date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Remover caracteres problemáticos
    df = df.applymap(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
    return df

def upsert_batch(table, batch):
    """Faz upsert de um lote de registros"""
    try:
        # Teste de serialização JSON
        json.dumps(batch)
        
        response = table.upsert(batch, on_conflict=['idcorretor']).execute()
        
        if hasattr(response, 'error') and response.error:
            print(f"❌ Erro no upsert: {response.error}")
            return False
        return True
    except Exception as e:
        print(f"❌ Erro crítico no upsert: {str(e)}")
        print("Registro problemático:", batch[0] if batch else "Nenhum")
        return False

# =======================================================
# Função Principal (MANTIDA)
# =======================================================
def main():
    try:
        print(f"⏳ Iniciando sincronização em {datetime.now().isoformat()}")
        
        # 1. Coleta de dados da API
        url_corretor = "https://coelho.cvcrm.com.br/api/v1/cvdw/corretores"
        
        initial_data = make_safe_request(
            url_corretor,
            payload={"pagina": 1, "registros_por_pagina": 500}
        )
        
        dfs = [pd.DataFrame(initial_data['dados'])]
        total_pages = initial_data.get('total_de_paginas', 1)
        
        for page in range(2, total_pages + 1):
            print(f"📄 Processando página {page}/{total_pages}...")
            page_data = make_safe_request(
                url_corretor,
                payload={"pagina": page, "registros_por_pagina": 500}
            )
            dfs.append(pd.DataFrame(page_data['dados']))
            time.sleep(1)

        # 2. Processamento dos dados
        df_corretor = pd.concat(dfs, ignore_index=True)
        cols_necessarias = ['idcorretor', 'ativo_login', 'nome', 'documento', 'data_cad', 'idimobiliaria']
        df_corretor = df_corretor[cols_necessarias].copy()
        df_corretor = prepare_data(df_corretor)
        
        print("🔍 Dados processados (amostra):")
        print(df_corretor.head(2))

        # 3. Upsert em lotes
        batch_size = 50
        dados = df_corretor.to_dict('records')
        total_registros = len(dados)
        
        print(f"🚀 Preparando upsert de {total_registros} registros...")
        
        for i in range(0, total_registros, batch_size):
            batch = dados[i:i + batch_size]
            print(f"⏳ Lote {i//batch_size + 1} ({len(batch)} registros)...")
            
            if not upsert_batch(tabela, batch):
                print("⚠️ Tentando upsert registro por registro...")
                for record in batch:
                    if not upsert_batch(tabela, [record]):
                        print(f"❌ Falha persistente no registro: {record.get('idcorretor')}")

        print(f"🎉 Processo concluído! Total de registros processados: {total_registros}")

    except Exception as e:
        print(f"❌ Falha crítica no processo principal: {str(e)}")
        raise

if __name__ == "__main__":
    main()
