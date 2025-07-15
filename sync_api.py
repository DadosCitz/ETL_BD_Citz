import pandas as pd
from datetime import datetime
import requests
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client

# =======================================================
# Configuração do Supabase
# =======================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
API_TOKEN = os.environ["API_TOKEN"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

# Headers ESSENCIAIS (substitua com os da sua API)
headers = {
    "accept": "application/json",
    "email": "thiago.almeida@citz.co",
    "token": API_TOKEN,
    "content-type": "application/json"
}

# =======================================================
# Função para Requisição Segura
# =======================================================
def make_safe_request(url, payload, attempt=1, max_attempts=3):
    try:
        response = session.get(
            url,
            json=payload,
            headers=headers,
            timeout=(10, 30)
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

# =======================================================
# Função principal
# =======================================================
def main():
    try:
        url_corretor = "https://coelho.cvcrm.com.br/api/v1/cvdw/corretores"
        
        # 1. Coleta de dados
        print("⏳ Obtendo metadados...")
        initial_data = make_safe_request(
            url_corretor,
            payload={"pagina": 1, "registros_por_pagina": 500}
        )

        paginas_corretor = initial_data.get('total_de_paginas', 1)
        print(f"📊 Total de páginas: {paginas_corretor}")

        # 2. Coleta paginada
        dfs = [pd.DataFrame(initial_data['dados'])]
        
        for pagina in range(2, paginas_corretor + 1):
            print(f"🔍 Processando página {pagina}/{paginas_corretor}...")
            page_data = make_safe_request(
                url_corretor,
                payload={"pagina": pagina, "registros_por_pagina": 500}
            )
            dfs.append(pd.DataFrame(page_data['dados']))
            time.sleep(1)

        # 3. Processamento final
        df_corretor = pd.concat(dfs, ignore_index=True)
        df_corretor = df_corretor[['idcorretor', 'ativo_login', 'nome', 'documento', 'data_cad', 'idimobiliaria']].copy()
        
        # 4. CONVERSÃO PARA STRING (CRÍTICO)
        df_corretor = df_corretor.astype(str)  # Converte TODAS as colunas para string
        
        # 5. Tratamento de valores nulos/vazios
        df_corretor = df_corretor.fillna('')
        
        # 6. Conversão para formato do Supabase
        dados_para_inserir = df_corretor.to_dict('records')
        
        # DEBUG: Verifique os primeiros registros
        print("🔍 Dados preparados para inserção (amostra):")
        print(dados_para_inserir[:2])

        # 7. Inserção no Supabase (em lotes de 100)
        batch_size = 100
        total_registros = len(dados_para_inserir)
        
        for i in range(0, total_registros, batch_size):
            batch = dados_para_inserir[i:i + batch_size]
            print(f"⏳ Inserindo lote {i//batch_size + 1}...")
            response = supabase.table("d_Corretores").insert(batch).execute()
            
            if hasattr(response, 'error') and response.error:
                print(f"❌ Erro no lote {i//batch_size + 1}: {response.error}")
            else:
                print(f"✅ Lote {i//batch_size + 1} inserido (registros {i}-{min(i+batch_size, total_registros)})")

        print(f"🎉 Concluído! Total de registros processados: {total_registros}")

    except Exception as e:
        print(f"❌ Falha crítica: {str(e)}")
        raise

if __name__ == "__main__":
    main()
