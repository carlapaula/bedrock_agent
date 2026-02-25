"""
Script para extrair dados do Athena dentro de um path no s3
- Tratamento remove dados nulos e linhas vazias
- Extrai 100 linhas por tabela
- Salva em CSV na pasta sample_data local
"""
import boto3
import sys
import time
import os
import pandas as pd

def extrair_tabela(tabela, database, bucket_s3, regiao):
    """Extrai dados de uma tabela do Athena e salva localmente"""
    
    print(f"DEBUG - Tabela: '{tabela}'")
    print(f"DEBUG - Database: '{database}'")
    
    # Conectar ao Athena e S3
    athena = boto3.client('athena', region_name=regiao)
    s3 = boto3.client('s3', region_name=regiao)
    
    # Montar query SQL (pega 200 linhas para garantir 100 distintas)
    query = f"SELECT * FROM {database}.{tabela} LIMIT 200"
    
    print(f"DEBUG - Query: '{query}'")
    
    print(f"Executando query na tabela {tabela}...")
    
    # Executar query no Athena
    resposta = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': f's3://{bucket_s3}/athena-results/'}
    )
    
    query_id = resposta['QueryExecutionId']
    
    # Aguardar conclusão da query
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        estado = status['QueryExecution']['Status']['State']
        
        if estado == 'SUCCEEDED':
            print(f"Query concluída com sucesso!")
            break
        elif estado in ['FAILED', 'CANCELLED']:
            erro = status['QueryExecution']['Status'].get('StateChangeReason', 'Erro desconhecido')
            raise Exception(f"Query falhou: {erro}")
        
        time.sleep(2)
    
    # Baixar resultado para pasta sample_data local
    arquivo_s3 = f'athena-results/{query_id}.csv'
    pasta_local = os.path.join(os.path.dirname(__file__), '..', 'sample_data')
    os.makedirs(pasta_local, exist_ok=True)
    arquivo_local = os.path.join(pasta_local, f'{tabela}.csv')
    
    s3.download_file(bucket_s3, arquivo_s3, arquivo_local)
    
    # Tratar dados: remover duplicatas e nulos
    df = pd.read_csv(arquivo_local)
    df = df.drop_duplicates()
    df = df.dropna(how='all')
    df = df.head(100)
    df.to_csv(arquivo_local, index=False)
    
    print(f" Arquivo salvo em: {arquivo_local}")
    print(f" Linhas únicas: {len(df)}\n")

# Execução principal
if __name__ == '__main__':
    # Validar argumentos
    if len(sys.argv) < 3:
        print("Uso: python athena_extract.py <tabelas> <bucket_s3> [database] [regiao]")
        print("\nExemplos:")
        print("  python amostra_local.py minha_tabela meu-bucket")
        print("  python amostra_local.py tabela1,tabela2 meu-bucket dp_trusted us-east-1")
        sys.exit(1)
    
    # Parâmetros
    tabelas = sys.argv[1].split(',')  # Separar múltiplas tabelas por vírgula
    bucket_s3 = sys.argv[2]
    database = sys.argv[3] if len(sys.argv) > 3 else 'dp_trusted_dev'  # Inserir o database
    regiao = sys.argv[4] if len(sys.argv) > 4 else 'us-east-1'
    
    print(f"Database: {database}")
    print(f"Bucket S3: {bucket_s3}")
    print(f"Região: {regiao}\n")
    
    # Processar cada tabela
    for tabela in tabelas:
        extrair_tabela(tabela.strip(), database, bucket_s3, regiao)
