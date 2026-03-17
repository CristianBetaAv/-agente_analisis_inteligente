"""
Script para crear el índice en Azure AI Search y subir los datos de torres
"""

import json
import os
import sys
from pathlib import Path

# Añadir el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

# Configuración
SEARCH_SERVICE_NAME = "search-agente-perxia-dev"
SEARCH_ENDPOINT = f"https://{SEARCH_SERVICE_NAME}.search.windows.net"
INDEX_NAME = "torres-index"
API_VERSION = "2024-07-01"

def get_search_key():
    """Obtiene la clave de Azure AI Search desde variable de entorno o la solicita"""
    key = os.getenv("AZURE_SEARCH_KEY")
    if not key:
        print("⚠️ AZURE_SEARCH_KEY no encontrada en variables de entorno")
        key = input("Ingresa la clave de Azure AI Search: ").strip()
    return key


def create_index(search_key: str) -> bool:
    """Crea el índice en Azure AI Search con el schema para torres"""
    
    index_schema = {
        "name": INDEX_NAME,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "filterable": True},
            {"name": "team_name", "type": "Edm.String", "searchable": True, "filterable": True, "sortable": True},
            {"name": "tower", "type": "Edm.String", "searchable": True, "filterable": True, "facetable": True},
            {"name": "description", "type": "Edm.String", "searchable": True},
            {"name": "team_lead", "type": "Edm.String", "searchable": True, "filterable": True},
            {"name": "team_lead_email", "type": "Edm.String", "filterable": True},
            {"name": "skills", "type": "Collection(Edm.String)", "searchable": True, "filterable": True},
            {"name": "expertise_areas", "type": "Collection(Edm.String)", "searchable": True, "filterable": True},
            {"name": "technologies", "type": "Collection(Edm.String)", "searchable": True, "filterable": True},
            {"name": "frameworks", "type": "Collection(Edm.String)", "searchable": True, "filterable": True},
            # Campo combinado para búsqueda semántica
            {"name": "full_text", "type": "Edm.String", "searchable": True}
        ],
        "suggesters": [
            {
                "name": "sg",
                "searchMode": "analyzingInfixMatching",
                "sourceFields": ["team_name", "tower", "description"]
            }
        ],
        "scoringProfiles": [
            {
                "name": "boostTechnologies",
                "text": {
                    "weights": {
                        "skills": 3,
                        "technologies": 2,
                        "expertise_areas": 2.5,
                        "description": 1.5,
                        "full_text": 1
                    }
                }
            }
        ],
        "defaultScoringProfile": "boostTechnologies"
    }
    
    headers = {
        "Content-Type": "application/json",
        "api-key": search_key
    }
    
    url = f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}?api-version={API_VERSION}"
    
    print(f"📋 Creando índice '{INDEX_NAME}'...")
    
    # Intentar eliminar índice existente
    delete_response = requests.delete(url, headers=headers)
    if delete_response.status_code == 204:
        print("🗑️ Índice anterior eliminado")
    
    # Crear nuevo índice
    create_url = f"{SEARCH_ENDPOINT}/indexes?api-version={API_VERSION}"
    response = requests.post(create_url, headers=headers, json=index_schema)
    
    if response.status_code in [200, 201]:
        print(f"✅ Índice '{INDEX_NAME}' creado exitosamente")
        return True
    else:
        print(f"❌ Error creando índice: {response.status_code}")
        print(response.text)
        return False


def upload_documents(search_key: str) -> bool:
    """Sube los documentos de torres_data.json al índice"""
    
    # Cargar datos
    data_path = Path(__file__).parent.parent / "data" / "torres_data.json"
    
    print(f"📂 Cargando datos desde: {data_path}")
    
    with open(data_path, "r", encoding="utf-8") as f:
        torres = json.load(f)
    
    print(f"📊 {len(torres)} torres encontradas")
    
    # Preparar documentos para Azure Search
    documents = []
    for torre in torres:
        # Crear campo de texto completo para búsqueda
        full_text_parts = [
            torre.get("team_name", ""),
            torre.get("tower", ""),
            torre.get("description", ""),
            " ".join(torre.get("skills", [])),
            " ".join(torre.get("expertise_areas", [])),
            " ".join(torre.get("technologies", [])),
            " ".join(torre.get("frameworks", []))
        ]
        
        doc = {
            "@search.action": "upload",
            "id": str(torre.get("id")),
            "team_name": torre.get("team_name", ""),
            "tower": torre.get("tower", ""),
            "description": torre.get("description", ""),
            "team_lead": torre.get("team_lead", ""),
            "team_lead_email": torre.get("team_lead_email", ""),
            "skills": torre.get("skills", []),
            "expertise_areas": torre.get("expertise_areas", []),
            "technologies": torre.get("technologies", []),
            "frameworks": torre.get("frameworks", []),
            "full_text": " ".join(full_text_parts)
        }
        documents.append(doc)
    
    # Subir documentos
    headers = {
        "Content-Type": "application/json",
        "api-key": search_key
    }
    
    url = f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}/docs/index?api-version={API_VERSION}"
    
    payload = {"value": documents}
    
    print(f"📤 Subiendo {len(documents)} documentos...")
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        result = response.json()
        success_count = sum(1 for r in result.get("value", []) if r.get("status"))
        print(f"✅ {success_count}/{len(documents)} documentos indexados exitosamente")
        return True
    else:
        print(f"❌ Error subiendo documentos: {response.status_code}")
        print(response.text)
        return False


def test_search(search_key: str):
    """Realiza una búsqueda de prueba"""
    
    headers = {
        "Content-Type": "application/json",
        "api-key": search_key
    }
    
    # Búsqueda de prueba
    search_query = {
        "search": "inteligencia artificial machine learning",
        "top": 3,
        "select": "team_name,tower,description"
    }
    
    url = f"{SEARCH_ENDPOINT}/indexes/{INDEX_NAME}/docs/search?api-version={API_VERSION}"
    
    print("\n🔍 Prueba de búsqueda: 'inteligencia artificial machine learning'")
    
    response = requests.post(url, headers=headers, json=search_query)
    
    if response.status_code == 200:
        results = response.json()
        print(f"📊 {len(results.get('value', []))} resultados encontrados:\n")
        
        for i, doc in enumerate(results.get("value", []), 1):
            print(f"  {i}. {doc.get('tower')} ({doc.get('team_name')})")
            print(f"     {doc.get('description', '')[:100]}...")
            print()
    else:
        print(f"❌ Error en búsqueda: {response.status_code}")


def main():
    print("=" * 60)
    print("🚀 Configuración de Azure AI Search - Torres Index")
    print("=" * 60)
    print()
    
    search_key = get_search_key()
    
    if not search_key:
        print("❌ No se proporcionó clave de búsqueda")
        return
    
    # Paso 1: Crear índice
    if not create_index(search_key):
        return
    
    # Paso 2: Subir documentos
    if not upload_documents(search_key):
        return
    
    # Paso 3: Probar búsqueda
    test_search(search_key)
    
    print("\n" + "=" * 60)
    print("✅ Configuración completada exitosamente")
    print("=" * 60)
    print(f"\n📌 Endpoint: {SEARCH_ENDPOINT}")
    print(f"📌 Índice: {INDEX_NAME}")


if __name__ == "__main__":
    main()
