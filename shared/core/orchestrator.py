"""
Orquestador principal para el análisis inteligente de oportunidades
Coordina el flujo completo desde la recepción del payload hasta la respuesta
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Imports perezosos para evitar errores de carga
# from ..models.opportunity import OpportunityPayload
# from ..services.openai_service import OpenAIService
# from ..services.search_service import SearchService
# from ..services.blob_storage_service import BlobStorageService
# from ..services.cosmos_service import CosmosDBService
# from ..generators.adaptive_card import generate_opportunity_card
# from ..generators.pdf_generator import PDFGenerator


class OpportunityOrchestrator:
    """
    Orquestador para el análisis de oportunidades de Dynamics 365.
    
    Flujo:
    1. Recibir payload de Power Automate
    2. Validar y parsear datos de oportunidad
    3. Buscar equipos relevantes en Azure AI Search
    4. Analizar con Azure OpenAI (gpt-4o-mini)
    5. Generar PDF del análisis
    6. Guardar en Cosmos DB
    7. Generar Adaptive Card para Teams
    8. Retornar respuesta estructurada
    """
    
    def __init__(self):
        """Inicializa los servicios necesarios"""
        try:
            from ..services.openai_service import OpenAIService
            from ..services.search_service import SearchService
            from ..services.blob_storage_service import BlobStorageService
            from ..services.cosmos_service import CosmosDBService
            
            self.openai_service = OpenAIService()
            self.search_service = SearchService()
            self.blob_service = BlobStorageService()
            
            # Cosmos DB es opcional
            try:
                self.cosmos_service = CosmosDBService()
                self.cosmos_enabled = True
            except Exception as e:
                logging.warning(f"⚠️ Cosmos DB no configurado: {str(e)}")
                self.cosmos_service = None
                self.cosmos_enabled = False
            
            logging.info("✅ OpportunityOrchestrator inicializado")
        except ImportError as ie:
            logging.error(f"❌ Error importando servicios: {str(ie)}")
            raise
    
    async def process_opportunity(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Procesa una oportunidad recibida desde Power Automate
        
        Args:
            payload: Datos de la oportunidad desde Dataverse/Power Automate
            
        Returns:
            Diccionario con el resultado del análisis
        """
        start_time = datetime.utcnow()
        
        try:
            # ========================================
            # PASO 1: Validar y parsear payload
            # ========================================
            logging.info("📥 Paso 1: Validando payload...")
            
            try:
                from ..models.opportunity import OpportunityPayload
                
                opportunity = OpportunityPayload(**payload)
            except ImportError as ie:
                logging.error(f"❌ Error importando pydantic/OpportunityPayload: {str(ie)}")
                return self._error_response(
                    "DEPENDENCY_ERROR",
                    f"Falta la librería pydantic. Instala con: pip install pydantic>=2.5.0",
                    payload.get("opportunityid", "unknown"),
                    payload.get("name", "Unknown")
                )
            except Exception as e:
                logging.error(f"❌ Error validando payload: {str(e)}")
                return self._error_response(
                    "VALIDATION_ERROR",
                    f"Error validando datos de oportunidad: {str(e)}",
                    payload.get("opportunityid", "unknown"),
                    payload.get("name", "Unknown")
                )
            
            logging.info(f"✅ Oportunidad validada: {opportunity.name}")
            
            # ========================================
            # PASO 2: Preparar texto para análisis
            # ========================================
            logging.info("📝 Paso 2: Preparando texto para análisis...")
            
            analysis_text = opportunity.format_for_analysis()
            logging.info(f"📝 Texto preparado: {len(analysis_text)} caracteres")
            
            # Añadir documento adicional si existe (modo actualización)
            if hasattr(opportunity, 'document_text') and opportunity.document_text:
                analysis_text += f"\n\n--- CONTEXTO ADICIONAL DEL DOCUMENTO ---\n{opportunity.document_text}"
                logging.info(f"📄 Documento añadido al análisis: +{len(opportunity.document_text)} caracteres")
            
            # Añadir análisis previo si existe (modo actualización)
            if hasattr(opportunity, 'previous_analysis') and opportunity.previous_analysis:
                previous_analysis_text = json.dumps(opportunity.previous_analysis.get('analysis', {}), ensure_ascii=False, indent=2)
                analysis_text += f"\n\n--- ANÁLISIS PREVIO ---\n{previous_analysis_text}"
                logging.info(f"📊 Análisis previo añadido: +{len(previous_analysis_text)} caracteres")
            
            # ========================================
            # PASO 3: Buscar equipos relevantes
            # ========================================
            logging.info("🔍 Paso 3: Buscando equipos relevantes...")
            
            # Usar la descripción limpia como query de búsqueda
            search_query = opportunity.clean_description[:500] if opportunity.clean_description else opportunity.name
            
            teams = self.search_service.search_teams(search_query, top=15)
            
            if not teams:
                logging.warning("⚠️ No se encontraron equipos, obteniendo todos...")
                teams = self.search_service.get_all_teams()
            
            logging.info(f"✅ {len(teams)} equipos encontrados")
            
            # ========================================
            # PASO 4: Análisis con IA
            # ========================================
            logging.info("🧠 Paso 4: Analizando con Azure OpenAI...")
            
            # Determinar si es actualización o análisis inicial
            is_update = hasattr(opportunity, 'document_text') and opportunity.document_text and hasattr(opportunity, 'previous_analysis') and opportunity.previous_analysis
            
            if is_update:
                logging.info("🔄 Modo actualización: refinando análisis con documento adicional")
                analysis_result = self.openai_service.analyze_opportunity_update(
                    opportunity_text=analysis_text,
                    available_teams=teams
                )
            else:
                logging.info("🆕 Modo inicial: análisis completo de oportunidad")
                analysis_result = self.openai_service.analyze_opportunity(
                    opportunity_text=analysis_text,
                    available_teams=teams
                )
            
            if not analysis_result:
                logging.error("❌ El análisis de IA no retornó resultados")
                return self._error_response(
                    "AI_ANALYSIS_ERROR",
                    "No se pudo completar el análisis con IA",
                    opportunity.opportunityid,
                    opportunity.name
                )
            
            logging.info("✅ Análisis completado")
            
            # ========================================
            # PASO 5: Procesar torres recomendadas
            # ========================================
            logging.info("🏗️ Paso 5: Procesando torres recomendadas...")
            
            # Normalizar torres del análisis
            required_towers = analysis_result.get("required_towers", [])
            team_recommendations = analysis_result.get("team_recommendations", [])
            
            # Enriquecer con datos de equipos encontrados
            enriched_teams = self._enrich_team_recommendations(team_recommendations, teams)
            analysis_result["team_recommendations"] = enriched_teams
            
            logging.info(f"✅ {len(required_towers)} torres requeridas, {len(enriched_teams)} equipos recomendados")
            
            # ========================================
            # PASO 6: Guardar en Cosmos DB (opcional)
            # ========================================
            cosmos_id = None
            if self.cosmos_enabled and self.cosmos_service:
                logging.info("💾 Paso 6: Guardando en Cosmos DB...")
                
                try:
                    record = {
                        "id": f"opp-{opportunity.opportunityid}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                        "opportunity_id": opportunity.opportunityid,
                        "opportunity_name": opportunity.name,
                        "event_type": opportunity.event_type,
                        "analysis": analysis_result,
                        "processed_at": datetime.utcnow().isoformat(),
                        "source": "power_automate"
                    }
                    
                    result = self.cosmos_service.save_analysis(record)
                    cosmos_id = result.get("id") if result else None
                    logging.info(f"✅ Guardado en Cosmos: {cosmos_id}")
                except Exception as e:
                    logging.warning(f"⚠️ Error guardando en Cosmos: {str(e)}")
            else:
                logging.info("⏭️ Paso 6: Cosmos DB no habilitado, saltando...")
            
            # ========================================
            # PASO 7: Generar PDF
            # ========================================
            logging.info("📄 Paso 7: Generando PDF...")
            
            pdf_url = None
            try:
                from ..generators.pdf_generator import PDFGenerator
                
                pdf_generator = PDFGenerator()
                pdf_bytes = pdf_generator.generate(
                    title=f"Análisis: {opportunity.name}",
                    analysis=analysis_result,
                    metadata={
                        "opportunity_id": opportunity.opportunityid,
                        "opportunity_name": opportunity.name,
                        "generated_at": datetime.utcnow().isoformat()
                    }
                )
                
                # Subir a Blob Storage
                blob_name = f"opportunity-analysis/{opportunity.opportunityid}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
                pdf_url = self.blob_service.upload_pdf(pdf_bytes, blob_name)
                logging.info(f"✅ PDF subido: {blob_name}")
                
            except ImportError as ie:
                logging.warning(f"⚠️ PDFGenerator no disponible: {str(ie)}")
            except Exception as e:
                logging.warning(f"⚠️ Error generando PDF: {str(e)}")
            
            # ========================================
            # PASO 8: Generar Adaptive Card
            # ========================================
            logging.info("🎨 Paso 8: Generando Adaptive Card...")
            
            try:
                from ..generators.adaptive_card import generate_opportunity_card
                
                adaptive_card = generate_opportunity_card(
                    opportunity_id=opportunity.opportunityid,
                    opportunity_name=opportunity.name,
                    analysis_data=analysis_result,
                    pdf_url=pdf_url
                )
                
                logging.info("✅ Adaptive Card generado")
            except ImportError as ie:
                logging.warning(f"⚠️ Adaptive Card no disponible: {str(ie)}")
                adaptive_card = None
            except Exception as e:
                logging.warning(f"⚠️ Error generando Adaptive Card: {str(e)}")
                adaptive_card = None
            
            # ========================================
            # PASO 9: Extraer líderes de torre únicos
            # ========================================
            tower_leaders = []
            seen_leaders = set()
            for team in enriched_teams:
                leader = team.get("team_lead", "")
                email = team.get("team_lead_email", "")
                tower = team.get("tower", "")
                team_name = team.get("team_name", "")
                
                if leader and leader not in seen_leaders:
                    seen_leaders.add(leader)
                    tower_leaders.append({
                        "tower": tower,
                        "team_name": team_name,
                        "leader_name": leader,
                        "leader_email": email
                    })
            
            logging.info(f"✅ {len(tower_leaders)} líderes de torre identificados")
            
            # ========================================
            # PASO 10: Construir respuesta
            # ========================================
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            response = {
                "success": True,
                "opportunity_id": opportunity.opportunityid,
                "opportunity_name": opportunity.name,
                "event_type": opportunity.event_type,
                
                "analysis": {
                    "executive_summary": analysis_result.get("executive_summary"),
                    "key_requirements": analysis_result.get("key_requirements", []),
                    "required_towers": required_towers,
                    "team_recommendations": enriched_teams,
                    "overall_risk_level": analysis_result.get("overall_risk_level"),
                    "risks": analysis_result.get("risks", []),
                    "timeline_estimate": analysis_result.get("timeline_estimate"),
                    "effort_estimate": analysis_result.get("effort_estimate"),
                    "recommendations": analysis_result.get("recommendations", []),
                    "next_steps": analysis_result.get("next_steps", []),
                    "clarification_questions": analysis_result.get("clarification_questions", []),
                    "confidence": analysis_result.get("analysis_confidence", 0.0)
                },
                
                "outputs": {
                    "adaptive_card": adaptive_card,
                    "pdf_url": pdf_url,
                    "cosmos_record_id": cosmos_id
                },
                
                "metadata": {
                    "processed_at": datetime.utcnow().isoformat(),
                    "processing_time_seconds": round(processing_time, 2),
                    "model_used": "GPT-4o-mini",
                    "teams_evaluated": len(teams)
                }
            }
            
            logging.info(f"✅ Procesamiento completado en {processing_time:.2f}s")
            return response
            
        except Exception as e:
            logging.error(f"❌ Error procesando oportunidad: {str(e)}")
            import traceback
            logging.error(f"❌ Traceback: {traceback.format_exc()}")
            
            return self._error_response(
                "PROCESSING_ERROR",
                str(e),
                payload.get("opportunityid", "unknown"),
                payload.get("name", "Unknown")
            )
    
    def _enrich_team_recommendations(
        self, 
        ai_recommendations: list, 
        search_results: list
    ) -> list:
        """
        Enriquece las recomendaciones de IA con datos reales de los equipos
        """
        enriched = []
        
        # Crear lookup de equipos por nombre/torre
        teams_lookup = {}
        for team in search_results:
            name = team.get("name", "").upper()
            tower = team.get("tower", "").upper()
            teams_lookup[name] = team
            teams_lookup[tower] = team
        
        for rec in ai_recommendations:
            if not isinstance(rec, dict):
                continue
            
            # Buscar equipo real
            team_name = rec.get("team_name", "").upper()
            tower = rec.get("tower", "").upper()
            
            real_team = teams_lookup.get(team_name) or teams_lookup.get(tower)
            
            if real_team:
                # Usar datos reales del equipo
                enriched.append({
                    "tower": real_team.get("tower", rec.get("tower")),
                    "team_name": real_team.get("name", rec.get("team_name")),
                    "team_lead": real_team.get("leader", rec.get("team_lead", "")),
                    "team_lead_email": real_team.get("leader_email", rec.get("team_lead_email", "")),
                    "relevance_score": rec.get("relevance_score", 0.8),
                    "matched_skills": rec.get("matched_skills", []),
                    "justification": rec.get("justification", ""),
                    "estimated_involvement": rec.get("estimated_involvement", "")
                })
            else:
                # Usar datos de la recomendación de IA
                enriched.append(rec)
        
        return enriched
    
    def _error_response(
        self, 
        code: str, 
        message: str, 
        opportunity_id: str,
        opportunity_name: str
    ) -> Dict[str, Any]:
        """Genera respuesta de error estructurada"""
        return {
            "success": False,
            "opportunity_id": opportunity_id,
            "opportunity_name": opportunity_name,
            "error": {
                "code": code,
                "message": message
            },
            "metadata": {
                "processed_at": datetime.utcnow().isoformat()
            }
        }
