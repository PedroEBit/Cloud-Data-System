import azure.functions as func
import logging
import pandas as pd
from io import BytesIO, StringIO
import os
from azure.storage.blob import BlobServiceClient
from sklearn.linear_model import LinearRegression

app = func.FunctionApp()

# Configuração

CONTAINER_ORIGEM = "meus-csvs"
CONTAINER_DESTINO = "dados-processados"

@app.blob_trigger(arg_name="myblob", path=f"{CONTAINER_ORIGEM}/{{name}}",
                  connection="AzureWebJobsStorage") 
def regressao(myblob: func.InputStream):
    logging.info(f"--- ARQUIVO DETECTADO: {myblob.name} ---")
    
    try:
        # 1. Leitura do CSV do Azure
        blob_bytes = myblob.read()
        df = pd.read_csv(BytesIO(blob_bytes))
        
        logging.info(f"Colunas encontradas: {df.columns.tolist()}")

        # 2. Regressão(Processamento)
        
        #Features
        x = df[['time-1', 'time-2', 'time-3', 'time-4', 'time-5']]
        #Target
        y = df['time']

        model = LinearRegression()
        model.fit(x, y)
        df['predict'] = model.predict(x)
        
        logging.info(f"Regressão concluída. Coeficientes: {model.coef_}")

        # 3. Salvando de df para csv
        output = StringIO()
        df.to_csv(output, index=False)
        
        # 4. Upload do Resultado
        connect_str = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        nome_original = myblob.name.split('/')[-1]
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_DESTINO, 
            blob=f"predicao_{nome_original}"
        )
        
        if not blob_client.exists():
            try:
                blob_service_client.create_container(CONTAINER_DESTINO)
            except:
                pass

        # Envio do arquivo final
        blob_client.upload_blob(output.getvalue().encode('utf-8'), overwrite=True)
        logging.info(f"--- SUCESSO: Arquivo salvo em {CONTAINER_DESTINO} ---")

    except Exception as e:
        logging.error(f"ERRO CRÍTICO: {str(e)}")