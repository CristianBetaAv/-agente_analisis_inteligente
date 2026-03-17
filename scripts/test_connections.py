"""
Script de prueba para verificar conexiones a los servicios Azure
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

print("=" * 60)
print("🔍 Verificación de Servicios Azure")
print("=" * 60)

# Verificar variables de entorno
print("\n📋 Variables de entorno:")
services = {
    "Azure OpenAI": {
        "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "key": os.getenv("AZURE_OPENAI_KEY"),
        "deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    },
    "Azure AI Search": {
        "endpoint": os.getenv("AZURE_SEARCH_ENDPOINT"),
        "key": os.getenv("AZURE_SEARCH_KEY"),
        "index": os.getenv("AZURE_SEARCH_INDEX_TEAMS")
    },
    "Azure Storage": {
        "connection": os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        "container": os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    },
    "Cosmos DB": {
        "endpoint": os.getenv("COSMOS_ENDPOINT"),
        "key": os.getenv("COSMOS_KEY"),
        "database": os.getenv("COSMOS_DATABASE_NAME")
    }
}

all_ok = True
for service_name, config in services.items():
    missing = [k for k, v in config.items() if not v]
    if missing:
        print(f"❌ {service_name}: Falta {', '.join(missing)}")
        all_ok = False
    else:
        print(f"✅ {service_name}: Configurado")

if not all_ok:
    print("\n⚠️ Faltan algunas configuraciones. Verifica el archivo .env")
    sys.exit(1)

print("\n" + "-" * 60)
print("🔌 Probando conexiones...")
print("-" * 60)

# 1. Probar Azure OpenAI
print("\n1️⃣ Azure OpenAI (GPT-4o-mini)...")
try:
    from openai import AzureOpenAI
    
    client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")
    )
    
    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini"),
        messages=[{"role": "user", "content": "Responde solo con 'OK' si puedes leer esto."}],
        max_tokens=10
    )
    
    result = response.choices[0].message.content.strip()
    print(f"   ✅ Respuesta: {result}")
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# 2. Probar Azure AI Search
print("\n2️⃣ Azure AI Search (torres-index)...")
try:
    from azure.search.documents import SearchClient
    from azure.core.credentials import AzureKeyCredential
    
    search_client = SearchClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        index_name=os.getenv("AZURE_SEARCH_INDEX_TEAMS"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))
    )
    
    results = search_client.search(search_text="inteligencia artificial", top=2)
    count = 0
    for result in results:
        count += 1
        print(f"   📌 {result.get('tower')} ({result.get('team_name')})")
    
    if count > 0:
        print(f"   ✅ {count} resultados encontrados")
    else:
        print("   ⚠️ Sin resultados (pero conexión OK)")
        
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# 3. Probar Azure Storage
print("\n3️⃣ Azure Blob Storage...")
try:
    from azure.storage.blob import BlobServiceClient
    
    blob_service = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
    container_client = blob_service.get_container_client(container_name)
    
    # Verificar que el contenedor existe
    if container_client.exists():
        print(f"   ✅ Contenedor '{container_name}' accesible")
    else:
        print(f"   ⚠️ Contenedor '{container_name}' no existe")
        
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

# 4. Probar Cosmos DB
print("\n4️⃣ Cosmos DB...")
try:
    from azure.cosmos import CosmosClient
    
    cosmos_client = CosmosClient(
        url=os.getenv("COSMOS_ENDPOINT"),
        credential=os.getenv("COSMOS_KEY")
    )
    
    database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE_NAME"))
    container = database.get_container_client(os.getenv("COSMOS_CONTAINER_NAME"))
    
    # Verificar acceso
    container.read()
    print(f"   ✅ Base de datos y contenedor accesibles")
    
except Exception as e:
    print(f"   ❌ Error: {str(e)}")

print("\n" + "=" * 60)
print("✅ Verificación completada")
print("=" * 60)
