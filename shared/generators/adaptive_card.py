"""
🎨 Generador de Adaptive Cards para Oportunidades
Diseño Corporativo Responsive - Compatible con Teams Desktop y Mobile
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime


def _get_risk_color(risk_level: str) -> str:
    """Retorna el color según el nivel de riesgo"""
    risk_level = (risk_level or "").lower()
    if risk_level in ['alto', 'high', 'crítico', 'critical']:
        return "Attention"
    elif risk_level in ['medio', 'medium', 'moderado', 'moderate']:
        return "Warning"
    else:
        return "Good"


def _get_risk_badge(risk_level: str) -> str:
    """Retorna badge de riesgo"""
    risk_level = (risk_level or "").lower()
    if risk_level in ['alto', 'high', 'crítico', 'critical']:
        return "🔴 ALTO"
    elif risk_level in ['medio', 'medium', 'moderado', 'moderate']:
        return "🟡 MEDIO"
    else:
        return "🟢 BAJO"


def _truncate_text(text: str, max_length: int = 300) -> str:
    """Trunca texto de forma segura"""
    if not text:
        return ""
    text = str(text)
    if len(text) > max_length:
        return text[:max_length - 3] + "..."
    return text


def _create_team_card(team: Dict, index: int) -> Dict:
    """Crea una tarjeta visual para un equipo recomendado con líder y justificación"""
    if not isinstance(team, dict):
        return None
    
    team_name = team.get("team_name", "Equipo")
    tower = team.get("tower", "")
    score = team.get("relevance_score", 0)
    score_pct = int(score * 100) if isinstance(score, (int, float)) else 0
    justification = _truncate_text(team.get("justification", ""), 150)
    team_lead = team.get("team_lead", "")
    team_lead_email = team.get("team_lead_email", "")
    
    # Determinar color según score
    if score_pct >= 80:
        score_color = "Good"
        badge_text = "⭐ TOP"
    elif score_pct >= 60:
        score_color = "Warning"
        badge_text = "✓ MATCH"
    else:
        score_color = "Default"
        badge_text = ""
    
    # Items base
    card_items = [
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": f"**{team_name}**",
                            "wrap": True,
                            "size": "Medium",
                            "color": "Accent" if index == 0 else "Default"
                        },
                        {
                            "type": "TextBlock",
                            "text": tower,
                            "size": "Small",
                            "isSubtle": True,
                            "spacing": "None"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": f"**{score_pct}%**",
                            "size": "Large",
                            "weight": "Bolder",
                            "color": score_color,
                            "horizontalAlignment": "Right"
                        }
                    ],
                    "verticalContentAlignment": "Center"
                }
            ]
        }
    ]
    
    # Agregar líder si existe
    if team_lead:
        leader_text = f"👤 {team_lead}"
        if team_lead_email:
            leader_text += f" • 📧 {team_lead_email}"
        card_items.append({
            "type": "TextBlock",
            "text": leader_text,
            "size": "Small",
            "isSubtle": True,
            "spacing": "Small"
        })
    
    # Agregar justificación si existe
    if justification:
        card_items.append({
            "type": "TextBlock",
            "text": f"💡 _{justification}_",
            "wrap": True,
            "size": "Small",
            "spacing": "Small"
        })
    
    return {
        "type": "Container",
        "style": "emphasis" if index == 0 else "default",
        "spacing": "Medium",
        "separator": index > 0,
        "items": card_items
    }


def _create_risk_item(risk: Dict) -> Dict:
    """Crea un item de riesgo con color visual"""
    if not isinstance(risk, dict):
        return None
    
    level = risk.get("level", "medio")
    description = _truncate_text(risk.get("description", ""), 150)
    mitigation = _truncate_text(risk.get("mitigation", ""), 100)
    
    # Determinar estilo según nivel
    level_lower = (level or "").lower()
    if level_lower in ['alto', 'high', 'crítico', 'critical']:
        container_style = "attention"
    elif level_lower in ['medio', 'medium', 'moderado', 'moderate']:
        container_style = "warning"
    else:
        container_style = "good"
    
    items = [
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": _get_risk_badge(level),
                            "size": "Small",
                            "weight": "Bolder"
                        }
                    ]
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": description,
                            "wrap": True,
                            "size": "Small"
                        }
                    ]
                }
            ]
        }
    ]
    
    # Agregar mitigación si existe
    if mitigation:
        items.append({
            "type": "TextBlock",
            "text": f"🛡️ _{mitigation}_",
            "wrap": True,
            "size": "Small",
            "isSubtle": True,
            "spacing": "Small"
        })
    
    return {
        "type": "Container",
        "style": container_style,
        "spacing": "Small",
        "items": items
    }


def generate_opportunity_card(
    opportunity_id: str,
    opportunity_name: str,
    analysis_data: Dict[str, Any],
    pdf_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Genera un Adaptive Card corporativo y responsive para análisis de oportunidades
    
    Optimizado para:
    - Microsoft Teams Desktop
    - Microsoft Teams Mobile
    - Outlook
    
    Args:
        opportunity_id: ID de la oportunidad
        opportunity_name: Nombre de la oportunidad
        analysis_data: Datos del análisis de IA
        pdf_url: URL del PDF generado (opcional)
        
    Returns:
        Diccionario con el Adaptive Card
    """
    try:
        logging.info("🎨 Generando Adaptive Card corporativo...")
        
        # Validación de datos
        if not analysis_data:
            analysis_data = {}
        
        # Extraer datos del análisis
        exec_summary = _truncate_text(analysis_data.get("executive_summary", "Análisis en proceso"), 400)
        key_requirements = analysis_data.get("key_requirements", [])[:5]
        teams = analysis_data.get("team_recommendations", [])[:4]
        risks = analysis_data.get("risks", [])[:3]
        overall_risk = analysis_data.get("overall_risk_level", "Medio")
        timeline = analysis_data.get("timeline_estimate", {})
        effort = analysis_data.get("effort_estimate", {})
        recommendations = analysis_data.get("recommendations", [])[:4]
        next_steps = analysis_data.get("next_steps", [])[:4]
        clarification_questions = analysis_data.get("clarification_questions", [])[:3]
        confidence = analysis_data.get("analysis_confidence", 0.75)
        
        body = []
        
        # ========================================
        # 🏢 HEADER CORPORATIVO
        # ========================================
        body.append({
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "spacing": "None",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "🎯 ANÁLISIS INTELIGENTE DE OPORTUNIDAD",
                    "weight": "Bolder",
                    "size": "Medium",
                    "color": "Accent",
                    "spacing": "None"
                },
                {
                    "type": "TextBlock",
                    "text": _truncate_text(opportunity_name, 100),
                    "weight": "Bolder",
                    "size": "Large",
                    "wrap": True,
                    "spacing": "Small",
                    "color": "Light"
                },
                {
                    "type": "ColumnSet",
                    "spacing": "Medium",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"📋 ID: {opportunity_id}",
                                    "size": "Small",
                                    "isSubtle": True
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Center"
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"Riesgo: {_get_risk_badge(overall_risk)}",
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "horizontalAlignment": "Right"
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        
        # ========================================
        # 📊 RESUMEN EJECUTIVO
        # ========================================
        body.append({
            "type": "Container",
            "spacing": "Medium",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "📊 RESUMEN EJECUTIVO",
                    "weight": "Bolder",
                    "size": "Small",
                    "color": "Accent"
                },
                {
                    "type": "TextBlock",
                    "text": exec_summary,
                    "wrap": True,
                    "size": "Small",
                    "spacing": "Small"
                }
            ]
        })
        
        # ========================================
        # 📈 MÉTRICAS CLAVE
        # ========================================
        min_hours = effort.get("min_hours", 0) if effort else 0
        max_hours = effort.get("max_hours", 0) if effort else 0
        complexity = effort.get("complexity", "Media") if effort else "Media"
        duration = timeline.get("total_duration", "Por definir") if timeline else "Por definir"
        confidence_pct = int(confidence * 100) if isinstance(confidence, (int, float)) else 75
        
        body.append({
            "type": "Container",
            "style": "emphasis",
            "spacing": "Medium",
            "items": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "1",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "⏱️ Duración",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Center"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": str(duration),
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "horizontalAlignment": "Center",
                                    "wrap": True
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "1",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "💪 Esfuerzo",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Center"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": f"{min_hours}-{max_hours}h" if min_hours and max_hours else "Por definir",
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "horizontalAlignment": "Center"
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "1",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "📊 Complejidad",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Center"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": str(complexity),
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "horizontalAlignment": "Center"
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "1",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "🎯 Confianza",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Center"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": f"{confidence_pct}%",
                                    "weight": "Bolder",
                                    "size": "Small",
                                    "horizontalAlignment": "Center",
                                    "color": "Good" if confidence_pct >= 70 else "Warning"
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        
        # ========================================
        # 👥 EQUIPOS RECOMENDADOS
        # ========================================
        if teams:
            team_items = []
            for idx, team in enumerate(teams[:4]):
                team_card = _create_team_card(team, idx)
                if team_card:
                    team_items.append(team_card)
            
            if team_items:
                body.append({
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "👥 EQUIPOS RECOMENDADOS",
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Accent"
                        }
                    ] + team_items
                })
        
        # ========================================
        # 📋 REQUERIMIENTOS CLAVE
        # ========================================
        if key_requirements:
            req_text = "\n".join([f"• {_truncate_text(req, 100)}" for req in key_requirements[:5] if req])
            if req_text:
                body.append({
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "📋 REQUERIMIENTOS CLAVE",
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Accent"
                        },
                        {
                            "type": "TextBlock",
                            "text": req_text,
                            "wrap": True,
                            "size": "Small",
                            "spacing": "Small"
                        }
                    ]
                })
        
        # ========================================
        # ⚠️ RIESGOS IDENTIFICADOS
        # ========================================
        if risks:
            risk_items = []
            for risk in risks[:3]:
                risk_item = _create_risk_item(risk)
                if risk_item:
                    risk_items.append(risk_item)
            
            if risk_items:
                body.append({
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "⚠️ RIESGOS IDENTIFICADOS",
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Accent"
                        }
                    ] + risk_items
                })
        
        # ========================================
        # 💡 RECOMENDACIONES
        # ========================================
        if recommendations:
            rec_text = "\n".join([f"→ {_truncate_text(rec, 100)}" for rec in recommendations[:4] if rec])
            if rec_text:
                body.append({
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "💡 RECOMENDACIONES",
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Accent"
                        },
                        {
                            "type": "TextBlock",
                            "text": rec_text,
                            "wrap": True,
                            "size": "Small",
                            "spacing": "Small"
                        }
                    ]
                })
        
        # ========================================
        # ❓ PUNTOS A CLARIFICAR
        # ========================================
        if clarification_questions:
            q_text = "\n".join([f"❓ {_truncate_text(q, 100)}" for q in clarification_questions[:3] if q])
            if q_text:
                body.append({
                    "type": "Container",
                    "style": "warning",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "PUNTOS A CLARIFICAR",
                            "weight": "Bolder",
                            "size": "Small"
                        },
                        {
                            "type": "TextBlock",
                            "text": q_text,
                            "wrap": True,
                            "size": "Small",
                            "spacing": "Small"
                        }
                    ]
                })
        
        # ========================================
        # 🎯 PRÓXIMOS PASOS
        # ========================================
        if next_steps:
            steps_text = "\n".join([f"{idx+1}. {_truncate_text(step, 100)}" for idx, step in enumerate(next_steps[:4]) if step])
            if steps_text:
                body.append({
                    "type": "Container",
                    "spacing": "Medium",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "🎯 PRÓXIMOS PASOS",
                            "weight": "Bolder",
                            "size": "Small",
                            "color": "Accent"
                        },
                        {
                            "type": "TextBlock",
                            "text": steps_text,
                            "wrap": True,
                            "size": "Small",
                            "spacing": "Small"
                        }
                    ]
                })
        
        # ========================================
        # ⚠️ DISCLAIMER
        # ========================================
        body.append({
            "type": "Container",
            "style": "warning",
            "spacing": "Large",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "⚠️ **AVISO IMPORTANTE**",
                    "weight": "Bolder",
                    "size": "Small"
                },
                {
                    "type": "TextBlock",
                    "text": "Este análisis fue generado automáticamente por Inteligencia Artificial (GPT-4o-mini). Las estimaciones, recomendaciones y asignaciones de equipos son sugerencias basadas en la información disponible y **pueden no ser precisas**. Se recomienda validar con los líderes de torre antes de tomar decisiones.",
                    "wrap": True,
                    "size": "Small",
                    "spacing": "Small"
                }
            ]
        })
        
        # ========================================
        # 📝 FOOTER CORPORATIVO
        # ========================================
        body.append({
            "type": "Container",
            "separator": True,
            "spacing": "Medium",
            "items": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "🤖 Análisis generado con GPT-4o-mini",
                                    "size": "Small",
                                    "isSubtle": True
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                                    "size": "Small",
                                    "isSubtle": True,
                                    "horizontalAlignment": "Right"
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        
        # ========================================
        # 🎴 CONSTRUIR CARD FINAL
        # ========================================
        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": body,
            "msteams": {
                "width": "Full"
            }
        }
        
        logging.info("✅ Adaptive Card corporativo generado exitosamente")
        return card
        
    except Exception as e:
        logging.error(f"❌ Error generando Adaptive Card: {str(e)}")
        return {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "Container",
                    "style": "attention",
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": "⚠️ Error en el análisis",
                            "weight": "Bolder",
                            "size": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Oportunidad: {opportunity_name}",
                            "wrap": True,
                            "size": "Small"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Error: {str(e)}",
                            "size": "Small",
                            "isSubtle": True,
                            "wrap": True
                        }
                    ]
                }
            ]
        }
