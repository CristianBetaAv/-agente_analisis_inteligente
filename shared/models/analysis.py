"""
Modelos de análisis para el agente de análisis inteligente
"""

from typing import List, Optional, Dict
from pydantic import BaseModel, Field


class TeamRecommendation(BaseModel):
    """Recomendación de equipo/torre para el proyecto"""
    tower: str = Field(..., description="Nombre de la torre")
    team_name: str = Field(..., description="Nombre del equipo")
    team_lead: str = Field(..., description="Líder del equipo")
    team_lead_email: str = Field(..., description="Email del líder")
    relevance_score: float = Field(..., ge=0, le=1, description="Score de relevancia (0-1)")
    justification: str = Field(..., description="Justificación de la recomendación")
    matching_skills: List[str] = Field(default_factory=list, description="Skills que coinciden")


class RiskAssessment(BaseModel):
    """Evaluación de riesgos del proyecto"""
    level: str = Field(..., description="Nivel de riesgo: bajo, medio, alto, crítico")
    factors: List[str] = Field(default_factory=list, description="Factores de riesgo identificados")
    mitigations: List[str] = Field(default_factory=list, description="Mitigaciones sugeridas")


class BudgetEstimate(BaseModel):
    """Estimación de presupuesto"""
    min_hours: int = Field(..., description="Horas mínimas estimadas")
    max_hours: int = Field(..., description="Horas máximas estimadas")
    complexity: str = Field(..., description="Complejidad: baja, media, alta, muy alta")
    assumptions: List[str] = Field(default_factory=list, description="Supuestos de la estimación")


class TimelinePhase(BaseModel):
    """Fase del timeline del proyecto"""
    phase_name: str = Field(..., description="Nombre de la fase")
    duration_weeks: int = Field(..., description="Duración en semanas")
    activities: List[str] = Field(default_factory=list, description="Actividades de la fase")
    deliverables: List[str] = Field(default_factory=list, description="Entregables de la fase")


class OpportunityAnalysis(BaseModel):
    """Análisis completo de una oportunidad"""
    
    # Identificación
    opportunity_id: str = Field(..., description="ID de la oportunidad")
    opportunity_name: str = Field(..., description="Nombre de la oportunidad")
    
    # Resumen ejecutivo
    executive_summary: str = Field(..., description="Resumen ejecutivo del análisis")
    
    # Análisis técnico
    technologies_identified: List[str] = Field(default_factory=list, description="Tecnologías identificadas")
    skills_required: List[str] = Field(default_factory=list, description="Skills requeridos")
    
    # Recomendaciones de equipos
    recommended_teams: List[TeamRecommendation] = Field(
        default_factory=list, 
        description="Equipos recomendados ordenados por relevancia"
    )
    
    # Evaluación de riesgos
    risk_assessment: Optional[RiskAssessment] = Field(None, description="Evaluación de riesgos")
    
    # Estimación
    budget_estimate: Optional[BudgetEstimate] = Field(None, description="Estimación de esfuerzo")
    
    # Timeline sugerido
    timeline_phases: List[TimelinePhase] = Field(default_factory=list, description="Fases del proyecto")
    
    # Recomendaciones adicionales
    recommendations: List[str] = Field(default_factory=list, description="Recomendaciones adicionales")
    
    # Preguntas pendientes
    clarification_questions: List[str] = Field(
        default_factory=list, 
        description="Preguntas que necesitan clarificación"
    )
    
    # Metadata
    analysis_timestamp: str = Field(..., description="Timestamp del análisis")
    model_used: str = Field(default="GPT-4o-mini", description="Modelo de IA usado")
    confidence_score: float = Field(default=0.0, ge=0, le=1, description="Score de confianza del análisis")


class AnalysisResponse(BaseModel):
    """Respuesta completa del análisis"""
    success: bool = Field(..., description="Indica si el análisis fue exitoso")
    analysis: Optional[OpportunityAnalysis] = Field(None, description="Análisis de la oportunidad")
    adaptive_card: Optional[Dict] = Field(None, description="Adaptive Card para Teams")
    pdf_url: Optional[str] = Field(None, description="URL del PDF generado")
    error: Optional[str] = Field(None, description="Mensaje de error si falló")
    
    # Metadata
    opportunity_id: str = Field(..., description="ID de la oportunidad procesada")
    opportunity_name: str = Field(..., description="Nombre de la oportunidad")
    processed_at: str = Field(..., description="Timestamp de procesamiento")


class ErrorResponse(BaseModel):
    """Respuesta de error"""
    success: bool = Field(default=False)
    error: Dict = Field(..., description="Detalles del error")
    metadata: Optional[Dict] = Field(None, description="Metadata adicional")
