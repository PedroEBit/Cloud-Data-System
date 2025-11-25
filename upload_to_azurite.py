import os
from azure.storage.blob import BlobServiceClient

# Nome do arquivo CSV que está no seu repositório
LOCAL_FILE_NAME = "PI_train.csv" 
CONTAINER_NAME = "data-trab" 

# --- GESTÃO DA STRING DE CONEXÃO ---

# ATENÇÃO: Se estiver rodando no GitHub Actions, 'os.environ.get' buscará o Secret.
# Se estiver rodando localmente (sem o Secret), essa variável será None e o script falhará
# com o 'raise ValueError', o que é bom para evitar o uso de credenciais erradas.
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

# --- Mude o nome da função para um nome mais genérico ---
def upload_file_to_storage():
    print(f"Iniciando upload de {LOCAL_FILE_NAME} para o Azure Storage...")

    try:
        if not CONNECTION_STRING:
             # Isso é crucial para falhar se o Secret não estiver configurado no GA
             raise ValueError("String de Conexão (AZURE_STORAGE_CONNECTION_STRING) não encontrada nas variáveis de ambiente.")
             
        # O BlobServiceClient usa a string de conexão para determinar o destino.
        blob_service_client = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )

        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        # Cria o contêiner se ele não existir
        try:
            container_client.create_container()
            print(f"Contêiner '{CONTAINER_NAME}' criado (se ainda não existisse).")
        except Exception as e:
            if "ContainerAlreadyExists" not in str(e) and "Conflict" not in str(e):
                raise
            
        # Faz o upload do arquivo (acessando-o localmente no runner do GA)
        with open(LOCAL_FILE_NAME, "rb") as data:
            container_client.upload_blob(name=LOCAL_FILE_NAME, data=data, overwrite=True)

        print(f"✅ Upload de '{LOCAL_FILE_NAME}' concluído com sucesso!")

    except Exception as ex:
        print(f"❌ Ocorreu um erro durante o upload: {ex}")
        exit(1)

if __name__ == "__main__":
    if not os.path.exists(LOCAL_FILE_NAME):
        print(f"❌ Erro: Arquivo '{LOCAL_FILE_NAME}' não encontrado no repositório.")
        exit(1)
        
    # --- CORREÇÃO AQUI: Chamar a função com o nome correto ---
    upload_file_to_storage()