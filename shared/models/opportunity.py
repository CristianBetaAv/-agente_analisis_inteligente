"""
Modelo de datos para oportunidades de Dynamics 365 / Dataverse
Recibido desde Power Automate via HTTP POST
"""

import re
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field


class OpportunityPayload(BaseModel):
    """
    Payload de una oportunidad recibida desde Power Automate.
    
    Power Automate envía el body completo del registro de Dataverse
    cuando se dispara el trigger "When a row is added, modified or deleted".
    """
    
    # Campos obligatorios
    opportunityid: str = Field(..., description="GUID único de la oportunidad")
    name: str = Field(..., description="Nombre de la oportunidad")
    
    # Campos opcionales de Dynamics 365
    description: Optional[str] = Field(None, description="Descripción general")
    
    # Campos personalizados (prefix cr807_ típico de Dynamics)
    cr807_descripciondelrequerimientofuncional: Optional[str] = Field(
        None, 
        alias="cr807_descripciondelrequerimientofuncional",
        description="Descripción técnica/funcional del requerimiento"
    )
    cr807_descripciondelrequerimientotecnico: Optional[str] = Field(
        None,
        alias="cr807_descripciondelrequerimientotecnico", 
        description="Descripción técnica adicional"
    )
    
    # Campos estándar de oportunidad
    estimatedclosedate: Optional[str] = Field(None, description="Fecha estimada de cierre")
    estimatedvalue: Optional[float] = Field(None, description="Valor estimado")
    budgetamount: Optional[float] = Field(None, description="Monto del presupuesto")
    statecode: Optional[int] = Field(0, description="Estado (0=Abierta, 1=Ganada, 2=Perdida)")
    statuscode: Optional[int] = Field(None, description="Código de estado detallado")
    
    # Datos del cliente
    customerid: Optional[str] = Field(None, description="ID del cliente (cuenta o contacto)")
    customername: Optional[str] = Field(None, alias="_customerid_value", description="Nombre del cliente")
    
    # Datos del propietario
    ownerid: Optional[str] = Field(None, alias="_ownerid_value", description="ID del propietario")
    ownername: Optional[str] = Field(None, description="Nombre del propietario")
    
    # Metadata de Power Automate
    SdkMessage: Optional[str] = Field(None, description="Tipo de evento (Create, Update, Delete)")
    
    # Timestamps
    createdon: Optional[str] = Field(None, description="Fecha de creación")
    modifiedon: Optional[str] = Field(None, description="Fecha de modificación")
    
    class Config:
        populate_by_name = True  # Permite usar alias
        extra = "allow"  # Permite campos adicionales no definidos
    
    @property
    def clean_description(self) -> str:
        """Retorna la descripción funcional limpiando HTML si existe"""
        text = self.cr807_descripciondelrequerimientofuncional or self.description or ""
        # Limpiar tags HTML si existen
        clean = re.sub(r'<[^>]+>', '', text)
        # Limpiar entidades HTML
        clean = clean.replace('&nbsp;', ' ').replace('&amp;', '&')
        clean = clean.replace('&lt;', '<').replace('&gt;', '>')
        return clean.strip()
    
    @property
    def state_name(self) -> str:
        """Nombre legible del estado"""
        states = {0: "Abierta", 1: "Ganada", 2: "Perdida"}
        return states.get(self.statecode, "Desconocido")
    
    @property
    def event_type(self) -> str:
        """Tipo de evento que disparó el trigger"""
        return self.SdkMessage or "Unknown"
    
    def format_for_analysis(self) -> str:
        """
        Formatea los datos de la oportunidad para análisis con IA.
        Genera un texto estructurado con toda la información relevante.
        """
        sections = []
        
        # Encabezado
        sections.append(f"# Oportunidad: {self.name}")
        sections.append(f"**ID:** {self.opportunityid}")
        sections.append(f"**Estado:** {self.state_name}")
        
        # Información del cliente
        if self.customername:
            sections.append(f"**Cliente:** {self.customername}")
        
        # Valores monetarios
        if self.estimatedvalue:
            sections.append(f"**Valor estimado:** ${self.estimatedvalue:,.2f}")
        if self.budgetamount:
            sections.append(f"**Presupuesto:** ${self.budgetamount:,.2f}")
        
        # Fechas
        if self.estimatedclosedate:
            sections.append(f"**Fecha estimada de cierre:** {self.estimatedclosedate}")
        
        sections.append("")  # Línea en blanco
        
        # Descripción general
        if self.description:
            sections.append("## Descripción General")
            sections.append(self._clean_html(self.description))
            sections.append("")
        
        # Descripción funcional (campo personalizado)
        if self.cr807_descripciondelrequerimientofuncional:
            sections.append("## Requerimiento Funcional")
            sections.append(self._clean_html(self.cr807_descripciondelrequerimientofuncional))
            sections.append("")
        
        # Descripción técnica (campo personalizado)
        if self.cr807_descripciondelrequerimientotecnico:
            sections.append("## Requerimiento Técnico")
            sections.append(self._clean_html(self.cr807_descripciondelrequerimientotecnico))
            sections.append("")
        
        return "\n".join(sections)
    
    def _clean_html(self, text: str) -> str:
        """Limpia etiquetas HTML del texto"""
        if not text:
            return ""
        clean = re.sub(r'<[^>]+>', '', text)
        clean = clean.replace('&nbsp;', ' ').replace('&amp;', '&')
        clean = clean.replace('&lt;', '<').replace('&gt;', '>')
        clean = clean.replace('&#160;', ' ')
        return clean.strip()
