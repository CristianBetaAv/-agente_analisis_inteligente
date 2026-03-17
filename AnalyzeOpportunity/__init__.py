"""
Azure Function: AnalyzeOpportunity

Recibe oportunidades desde Power Automate y retorna análisis inteligente.

Endpoint: POST /api/analyze
Payload: Body de oportunidad desde Dataverse

Modos de operación:
1. Análisis inicial: Solo datos de Dataverse
2. Actualización: Datos de Dataverse + documento adicional + análisis previo

Response:
{
    "success": true,
    "opportunity_id": "...",
    "opportunity_name": "...",
    "analysis": {...},
    "outputs": {
        "adaptive_card": {...},
        "pdf_url": "..."
    }
}
"""

import os
import sys
import json
import logging
from datetime import datetime, date
import azure.functions as func
import base64
from io import BytesIO

# Agregar shared al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from shared.core.orchestrator import OpportunityOrchestrator

logging.basicConfig(level=logging.INFO)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder que maneja datetime objects"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extrae texto de un archivo PDF"""
    try:
        from PyPDF2 import PdfReader
        
        pdf_file = BytesIO(pdf_bytes)
        reader = PdfReader(pdf_file)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text.strip()
    except ImportError:
        logging.error("❌ PyPDF2 no está instalado. Instala con: pip install PyPDF2")
        raise
    except Exception as e:
        logging.error(f"Error extrayendo texto de PDF: {str(e)}")
        return ""


def extract_text_from_word(docx_bytes: bytes) -> str:
    """Extrae texto de un archivo Word (DOCX)"""
    try:
        from docx import Document
        
        doc_file = BytesIO(docx_bytes)
        doc = Document(doc_file)
        text = ""
        for para in doc.paragraphs:
            text += para.text + "\n"
        return text.strip()
    except ImportError:
        logging.error("❌ python-docx no está instalado. Instala con: pip install python-docx")
        raise
    except Exception as e:
        logging.error(f"Error extrayendo texto de Word: {str(e)}")
        return ""


async def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger principal para análisis de oportunidades.
    
    Flujo:
    1. Power Automate detecta nueva oportunidad en Dataverse
    2. Power Automate envía HTTP POST con el body de la oportunidad
    3. Esta función procesa los datos con IA (GPT-4o-mini)
    4. Retorna análisis con Adaptive Card para Teams
    
    Modos de operación:
    - Análisis inicial: Solo datos de Dataverse
    - Actualización: Datos de Dataverse + documento Base64 + análisis previo
    
    Payload esperado (análisis inicial):
    {
        "body": {
            "opportunityid": "guid",
            "name": "Nombre de la oportunidad",
            "description": "...",
            "cr807_descripciondelrequerimientofuncional": "...",
            ... otros campos de Dynamics 365
        },
        "teams_id": "ID del equipo de Teams",
        "channel_id": "ID del canal de Teams"
    }
    
    Payload esperado (actualización):
    {
        "body": { ... campos de Dataverse ... },
        "teams_id": "ID del equipo de Teams",
        "channel_id": "ID del canal de Teams",
        "document_base64": "base64_encoded_content",
        "document_filename": "document.pdf",
        "previous_analysis_id": "id_del_analisis_guardado"
    }
    """
    logging.info("=" * 60)
    logging.info("🚀 AGENTE DE ANÁLISIS INTELIGENTE - Función iniciada")
    logging.info("=" * 60)
    
    try:
        # Validar método
        if req.method != "POST":
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": {
                        "code": "METHOD_NOT_ALLOWED",
                        "message": "Solo se acepta método POST"
                    }
                }),
                status_code=405,
                mimetype="application/json"
            )
        
        # Obtener payload
        try:
            payload = req.get_json()
        except ValueError as e:
            logging.error(f"❌ Error parseando JSON: {str(e)}")
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": {
                        "code": "INVALID_JSON",
                        "message": "El body de la petición no es un JSON válido"
                    }
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        if not payload:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": {
                        "code": "EMPTY_PAYLOAD",
                        "message": "El body de la petición está vacío"
                    }
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Extraer estructura: body, teams_id, channel_id
        # Soporta ambos formatos:
        # 1. Nuevo: { "body": {...}, "teams_id": "...", "channel_id": "..." }
        # 2. Legacy: { "opportunityid": "...", ... } (todo flat)
        
        if "body" in payload and isinstance(payload["body"], dict):
            # Nuevo formato estructurado
            opportunity_data = payload["body"]
            teams_id = payload.get("teams_id") or payload.get("teamsId")
            channel_id = payload.get("channel_id") or payload.get("channelId")
            logging.info("📦 Payload estructurado detectado (body + teams_id + channel_id)")
        else:
            # Formato legacy (flat)
            opportunity_data = payload
            teams_id = payload.get("teams_id") or payload.get("teamsId")
            channel_id = payload.get("channel_id") or payload.get("channelId")
            logging.info("📦 Payload flat detectado (legacy)")
        
        # Agregar teams_id y channel_id al opportunity_data para el orquestador
        opportunity_data["teams_id"] = teams_id
        opportunity_data["channel_id"] = channel_id
        
        # Validar campos requeridos
        if "opportunityid" not in opportunity_data:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "error": {
                        "code": "MISSING_OPPORTUNITY_ID",
                        "message": "El payload debe contener 'opportunityid' (dentro de 'body' o directamente)"
                    }
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        logging.info(f"📥 Oportunidad recibida: {opportunity_data.get('name', 'Sin nombre')}")
        logging.info(f"📥 ID: {opportunity_data.get('opportunityid')}")
        logging.info(f"📥 Evento: {opportunity_data.get('SdkMessage', 'N/A')}")
        logging.info(f"📥 Teams ID: {teams_id or 'N/A'}")
        logging.info(f"📥 Channel ID: {channel_id or 'N/A'}")
        
        # Crear orquestador temprano para acceder a servicios
        logging.info("⚙️ Inicializando orquestador...")
        orchestrator = OpportunityOrchestrator()
        
        # Verificar si es una actualización con documento
        document_base64 = payload.get("document_base64")
        if document_base64:
            logging.info("📄 Modo actualización detectado - procesando documento...")
            
            try:
                # Decodificar Base64
                decoded_bytes = base64.b64decode(document_base64)
                logging.info(f"📄 Documento decodificado: {len(decoded_bytes)} bytes")
                
                # Extraer texto según tipo
                filename = payload.get("document_filename", "").lower()
                if filename.endswith('.pdf'):
                    document_text = extract_text_from_pdf(decoded_bytes)
                elif filename.endswith(('.docx', '.doc')):
                    document_text = extract_text_from_word(decoded_bytes)
                else:
                    # Intentar detectar por contenido
                    if decoded_bytes.startswith(b'%PDF'):
                        document_text = extract_text_from_pdf(decoded_bytes)
                    elif decoded_bytes.startswith(b'PK\x03\x04'):  # DOCX es ZIP
                        document_text = extract_text_from_word(decoded_bytes)
                    else:
                        logging.error("Tipo de documento no soportado")
                        return func.HttpResponse(
                            json.dumps({
                                "success": False,
                                "error": {
                                    "code": "UNSUPPORTED_DOCUMENT_TYPE",
                                    "message": "Solo se soportan archivos PDF y Word (DOCX/DOC)"
                                }
                            }),
                            status_code=400,
                            mimetype="application/json"
                        )
                
                logging.info(f"📄 Texto extraído: {len(document_text)} caracteres")
                
                # Recuperar análisis previo
                previous_analysis_id = payload.get("previous_analysis_id")
                if previous_analysis_id:
                    if orchestrator.cosmos_service:
                        previous_analysis = orchestrator.cosmos_service.get_analysis_by_id(previous_analysis_id)
                        if previous_analysis:
                            logging.info(f"📊 Análisis previo recuperado: {previous_analysis_id}")
                        else:
                            logging.warning(f"⚠️ Análisis previo no encontrado: {previous_analysis_id}")
                    else:
                        previous_analysis = None
                        logging.warning("⚠️ Cosmos DB no disponible para recuperar análisis previo")
                else:
                    previous_analysis = None
                    logging.info("ℹ️ No se especificó previous_analysis_id")
                
                # Agregar al opportunity_data
                opportunity_data["document_text"] = document_text
                opportunity_data["previous_analysis"] = previous_analysis
                
            except ImportError as ie:
                logging.error(f"❌ Error de dependencia: {str(ie)}")
                return func.HttpResponse(
                    json.dumps({
                        "success": False,
                        "error": {
                            "code": "MISSING_DEPENDENCY",
                            "message": f"Falta una librería requerida: {str(ie)}. Asegúrate de que requirements.txt contiene PyPDF2>=3.0.1 y python-docx>=1.1.0 y que se han instalado correctamente.",
                            "details": "Verifica en la consola de Azure Functions que las dependencias se hayan instalado correctamente."
                        }
                    }),
                    status_code=500,
                    mimetype="application/json"
                )
            except Exception as e:
                logging.error(f"❌ Error procesando documento: {str(e)}")
                return func.HttpResponse(
                    json.dumps({
                        "success": False,
                        "error": {
                            "code": "DOCUMENT_PROCESSING_ERROR",
                            "message": f"Error procesando el documento: {str(e)}"
                        }
                    }),
                    status_code=400,
                    mimetype="application/json"
                )
        else:
            logging.info("📄 Modo análisis inicial")
        
        logging.info("🔄 Procesando oportunidad...")
        result = await orchestrator.process_opportunity(opportunity_data)
        
        # Determinar código de respuesta
        status_code = 200 if result.get("success", False) else 500
        
        logging.info("=" * 60)
        if result.get("success"):
            logging.info("✅ PROCESAMIENTO EXITOSO")
        else:
            logging.error("❌ PROCESAMIENTO FALLIDO")
        logging.info("=" * 60)
        
        return func.HttpResponse(
            json.dumps(result, cls=DateTimeEncoder, ensure_ascii=False, indent=2),
            status_code=status_code,
            mimetype="application/json",
            charset="utf-8"
        )
        
    except ImportError as e:
        logging.error(f"❌ ERROR CRÍTICO (dependencia faltante): {str(e)}")
        import traceback
        logging.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": {
                    "code": "MISSING_DEPENDENCY",
                    "message": str(e),
                    "type": type(e).__name__
                },
                "metadata": {
                    "processed_at": datetime.utcnow().isoformat()
                }
            }),
            status_code=500,
            mimetype="application/json",
            charset="utf-8"
        )
    except Exception as e:
        logging.error(f"❌ ERROR CRÍTICO: {str(e)}")
        import traceback
        logging.error(f"❌ TRACEBACK: {traceback.format_exc()}")
        
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": str(e),
                    "type": type(e).__name__
                },
                "metadata": {
                    "processed_at": datetime.utcnow().isoformat()
                }
            }),
            status_code=500,
            mimetype="application/json",
            charset="utf-8"
        )
