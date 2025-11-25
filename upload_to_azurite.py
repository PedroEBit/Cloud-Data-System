import os
from azure.storage.blob import BlobServiceClient

# Nome do arquivo CSV que está no seu repositório
LOCAL_FILE_NAME = "PI_train.csv" 
CONTAINER_NAME = "data-trab" 

# String de Conexão do Azurite
# Esta string é a string de conexão padrão/default para o Azurite (HTTP)
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtCdLwSjkQk/SUwa effectingCQN0YzWGuxqUgEazBPZXoPHhn/Lp2PS+zrksazssqrqicfA==;"
    "BlobEndpoint=http://127.0.0.1:10000;"
    "QueueEndpoint=http://127.0.0.1:10001;"
    "TableEndpoint=http://127.0.0.1:10002;" # Altere o host se não estiver usando 'azurite-container'
)

# --- CÓDIGO PARA MIGRAÇÃO (USE ISTO QUANDO FOR PARA A NUVEM) ---
# Quando estiver pronto para o Azure Real, o script deve ler a string de conexão
# de uma variável de ambiente (como a AZURE_STORAGE_CONNECTION_STRING, que você 
# configurará como um Secret no GitHub Actions).

CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", AZURITE_CONNECTION_STRING)
#
# Se a variável de ambiente for encontrada (como no GitHub Actions), ela usa a string real.
# Caso contrário (rodando localmente), ela volta para a string do Azurite.
# 
# Para manter a compatibilidade com o Azurite local (por enquanto), vamos usar a string fixa:

# CONNECTION_STRING = AZURITE_CONNECTION_STRING

def upload_file_to_azurite():
    print(f"Iniciando upload de {LOCAL_FILE_NAME} para Azurite...")

    try:
        if not CONNECTION_STRING:
             raise ValueError("String de Conexão não encontrada.")
        # 1. Conectar ao serviço de Blob
        blob_service_client = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )

        # 2. Obter o cliente do contêiner
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # O Azurite pode exigir que o contêiner seja criado antes do primeiro upload.
        # Tentativa segura de criar o contêiner
        try:
            container_client.create_container()
            print(f"Contêiner '{CONTAINER_NAME}' criado (se ainda não existisse).")
        except Exception as e:
            # Se o contêiner já existir, essa exceção será ignorada.
            if "ContainerAlreadyExists" not in str(e):
                raise
            
        # 3. Fazer o upload do arquivo
        with open(LOCAL_FILE_NAME, "rb") as data:
            container_client.upload_blob(name=LOCAL_FILE_NAME, data=data, overwrite=True)

        print(f"✅ Upload de '{LOCAL_FILE_NAME}' concluído com sucesso!")

    except Exception as ex:
        print(f"❌ Ocorreu um erro durante o upload: {ex}")
        # É crucial que o workflow falhe se o script falhar
        exit(1)

if __name__ == "__main__":
    if not os.path.exists(LOCAL_FILE_NAME):
        print(f"❌ Erro: Arquivo '{LOCAL_FILE_NAME}' não encontrado no repositório.")
        exit(1)
        
    upload_file_to_azurite()