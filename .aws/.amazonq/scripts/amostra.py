"""
Script para extrair dados do Athena dentro de um path no s3
- Tratamento remove dados nulos e linhas vazias
- Extrai 100 linhas por tabela
- Salva em CSV na pasta sample_data do S3
"""
import boto3
import sys
import time

def extrair_tabela(tabela, database, bucket_s3, regiao):
    """Extrai dados de uma tabela do Athena e salva no S3"""
    
    # Conectar ao Athena e S3
    athena = boto3.client('athena', region_name=regiao)
    s3 = boto3.client('s3', region_name=regiao)
    
    # Montar query SQL (remove nulos e limita a 200 linhas)
    query = f"""
    SELECT DISTINCT * FROM {database}.{tabela}
    WHERE NOT ({tabela}.* IS NULL)
    LIMIT 100
    """
    
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
    
    # Copiar resultado para pasta sample_data
    arquivo_origem = f'athena-results/{query_id}.csv'
    arquivo_destino = f'sample_data/{tabela}.csv'
    
    s3.copy_object(
        Bucket=bucket_s3,
        CopySource={'Bucket': bucket_s3, 'Key': arquivo_origem},
        Key=arquivo_destino
    )
    
    print(f" Arquivo salvo em: s3://{bucket_s3}/{arquivo_destino}\n")

# Execução principal
if __name__ == '__main__':
    # Validar argumentos
    if len(sys.argv) < 3:
        print("Uso: python athena_extract.py <tabelas> <bucket_s3> [database] [regiao]")
        print("\nExemplos:")
        print("  python athena_extract.py minha_tabela meu-bucket")
        print("  python athena_extract.py tabela1,tabela2 meu-bucket dp_trusted us-east-1")
        sys.exit(1)
    
    # Parâmetros
    tabelas = sys.argv[1].split('colaborar_edprod_edaluno')  #  inserir os nomes das tabelas 
    bucket_s3 = sys.argv[2]
    database = sys.argv[3] if len(sys.argv) > 3 else 'dp_trusted'  # Inserir o database
    regiao = sys.argv[4] if len(sys.argv) > 4 else 'us-east-1'
    
    print(f"Database: {database}")
    print(f"Bucket S3: {bucket_s3}")
    print(f"Região: {regiao}\n")
    
    # Processar cada tabela
    for tabela in tabelas:
        extrair_tabela(tabela.strip(), database, bucket_s3, regiao)
