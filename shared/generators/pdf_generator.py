"""
Generador de PDFs profesionales
"""

import logging
from typing import Dict, Any, Optional
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from io import BytesIO
from datetime import datetime


class PDFGenerator:
    """Clase para generar PDFs profesionales de análisis"""
    
    def __init__(self):
        self.styles = self._create_styles()
    
    def _create_styles(self):
        """Crea los estilos para el PDF"""
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#0078D4'),
            spaceAfter=30,
            alignment=TA_CENTER
        ))
        styles.add(ParagraphStyle(
            name='CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#0078D4'),
            spaceAfter=12,
            spaceBefore=12
        ))
        return styles
    
    def generate(
        self,
        title: str,
        analysis: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[bytes]:
        """
        Genera un PDF profesional del análisis
        
        Args:
            title: Título del documento
            analysis: Datos del análisis
            metadata: Metadatos opcionales
            
        Returns:
            Bytes del PDF generado
        """
        try:
            logging.info("📄 Generando PDF ejecutivo...")
            
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=72
            )
            
            story = []
            
            # Título principal
            story.append(Paragraph("RESUMEN EJECUTIVO", self.styles['CustomTitle']))
            if metadata and metadata.get('opportunity_id'):
                story.append(Paragraph(f"Oportunidad: {metadata['opportunity_id']}", self.styles['Normal']))
            story.append(Spacer(1, 0.2*inch))
            
            # Título del análisis
            story.append(Paragraph(f"<b>{title}</b>", self.styles['Heading2']))
            story.append(Spacer(1, 0.3*inch))
            
            # Fecha
            fecha = datetime.now().strftime("%d de %B de %Y")
            story.append(Paragraph(f"<i>Fecha de análisis: {fecha}</i>", self.styles['Normal']))
            story.append(Spacer(1, 0.4*inch))
            
            # Resumen ejecutivo
            story.append(Paragraph("RESUMEN EJECUTIVO", self.styles['CustomHeading']))
            exec_summary = analysis.get("executive_summary", analysis.get("summary", "No disponible"))
            story.append(Paragraph(str(exec_summary), self.styles['BodyText']))
            story.append(Spacer(1, 0.3*inch))
            
            # Puntos clave
            key_points = analysis.get("key_points", [])
            if key_points:
                story.append(Paragraph("PUNTOS CLAVE", self.styles['CustomHeading']))
                for point in key_points:
                    story.append(Paragraph(f"• {point}", self.styles['Normal']))
                    story.append(Spacer(1, 0.1*inch))
                story.append(Spacer(1, 0.2*inch))
            
            # Torres requeridas
            torres = analysis.get("required_towers", analysis.get("torres_requeridas", []))
            if torres:
                story.append(Paragraph("TORRES ORGANIZACIONALES REQUERIDAS", self.styles['CustomHeading']))
                torres_str = ", ".join(torres) if isinstance(torres, list) else str(torres)
                story.append(Paragraph(torres_str, self.styles['Normal']))
                story.append(Spacer(1, 0.3*inch))
            
            # Equipos recomendados
            teams = analysis.get("team_recommendations", analysis.get("equipos_recomendados", []))
            if teams:
                story.append(Paragraph("EQUIPOS RECOMENDADOS", self.styles['CustomHeading']))
                teams_data = [['Equipo', 'Torre', 'Confianza', 'Rol']]
                
                for team in teams[:5]:
                    teams_data.append([
                        team.get('team_name', team.get('nombre', '')),
                        team.get('tower', team.get('torre', '')),
                        f"{(team.get('confidence_score', team.get('confianza', 0)) or 0)*100:.0f}%",
                        team.get('recommended_role', team.get('rol', ''))
                    ])
                
                if len(teams_data) > 1:
                    teams_table = Table(teams_data, colWidths=[1.5*inch, 1*inch, 0.8*inch, 2*inch])
                    teams_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0078D4')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    story.append(teams_table)
                story.append(Spacer(1, 0.3*inch))
            
            # Riesgos principales
            risks = analysis.get("risks", analysis.get("riesgos", []))
            if risks:
                story.append(Paragraph("RIESGOS PRINCIPALES", self.styles['CustomHeading']))
                for risk in risks[:3]:
                    story.append(Paragraph(
                        f"<b>{risk.get('category', risk.get('categoria', ''))} ({risk.get('level', risk.get('nivel', ''))})</b>",
                        self.styles['Normal']
                    ))
                    story.append(Paragraph(risk.get('description', risk.get('descripcion', '')), self.styles['Normal']))
                    mitigation = risk.get('mitigation', risk.get('mitigacion', ''))
                    if mitigation:
                        story.append(Paragraph(f"<i>Mitigación: {mitigation}</i>", self.styles['Normal']))
                    story.append(Spacer(1, 0.15*inch))
                story.append(Spacer(1, 0.2*inch))
            
            # Estimaciones
            timeline = analysis.get('timeline_estimate', analysis.get('timeline', ''))
            budget = analysis.get('budget_estimate', analysis.get('presupuesto', {}))
            if timeline or budget:
                story.append(Paragraph("ESTIMACIONES", self.styles['CustomHeading']))
                if timeline:
                    story.append(Paragraph(f"<b>Timeline:</b> {timeline}", self.styles['Normal']))
                if budget:
                    if isinstance(budget, dict):
                        story.append(Paragraph(
                            f"<b>Rango de costo:</b> {budget.get('estimated_cost_range', budget.get('rango', 'N/A'))}",
                            self.styles['Normal']
                        ))
                        story.append(Paragraph(
                            f"<b>Horas estimadas:</b> {budget.get('estimated_hours', budget.get('horas', 'N/A'))}",
                            self.styles['Normal']
                        ))
                    else:
                        story.append(Paragraph(f"<b>Presupuesto:</b> {budget}", self.styles['Normal']))
                story.append(Spacer(1, 0.3*inch))
            
            # Recomendaciones
            recommendations = analysis.get("recommendations", analysis.get("recomendaciones", []))
            if recommendations:
                story.append(Paragraph("RECOMENDACIONES", self.styles['CustomHeading']))
                for rec in recommendations:
                    story.append(Paragraph(f"• {rec}", self.styles['Normal']))
                    story.append(Spacer(1, 0.1*inch))
            
            # Generar PDF
            doc.build(story)
            
            pdf_bytes = buffer.getvalue()
            buffer.close()
            
            logging.info(f"✅ PDF generado: {len(pdf_bytes)} bytes")
            return pdf_bytes
            
        except Exception as e:
            logging.error(f"❌ Error generando PDF: {str(e)}")
            return None


def generate_executive_pdf(
    work_item_id: int,
    title: str,
    analysis_data: Dict[str, Any]
) -> Optional[bytes]:
    """
    Genera un PDF ejecutivo profesional
    
    Args:
        work_item_id: ID del Work Item
        title: Título de la propuesta
        analysis_data: Datos del análisis
        
    Returns:
        Bytes del PDF generado
    """
    try:
        logging.info("📄 Generando PDF ejecutivo...")
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#0078D4'),
            spaceAfter=30,
            alignment=TA_CENTER
        ))
        styles.add(ParagraphStyle(
            name='CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#0078D4'),
            spaceAfter=12,
            spaceBefore=12
        ))
        
        # Contenido
        story = []
        
        # Título principal
        story.append(Paragraph("RESUMEN EJECUTIVO", styles['CustomTitle']))
        story.append(Paragraph(f"Work Item #{work_item_id}", styles['Normal']))
        story.append(Spacer(1, 0.2*inch))
        
        # Título de la propuesta
        story.append(Paragraph(f"<b>{title}</b>", styles['Heading2']))
        story.append(Spacer(1, 0.3*inch))
        
        # Fecha
        fecha = datetime.now().strftime("%d de %B de %Y")
        story.append(Paragraph(f"<i>Fecha de análisis: {fecha}</i>", styles['Normal']))
        story.append(Spacer(1, 0.4*inch))
        
        # Resumen ejecutivo
        story.append(Paragraph("RESUMEN EJECUTIVO", styles['CustomHeading']))
        exec_summary = analysis_data.get("executive_summary", "")
        story.append(Paragraph(exec_summary, styles['BodyText']))
        story.append(Spacer(1, 0.3*inch))
        
        # Puntos clave
        story.append(Paragraph("PUNTOS CLAVE", styles['CustomHeading']))
        for point in analysis_data.get("key_points", []):
            story.append(Paragraph(f"• {point}", styles['Normal']))
            story.append(Spacer(1, 0.1*inch))
        story.append(Spacer(1, 0.2*inch))
        
        # Torres requeridas
        story.append(Paragraph("TORRES ORGANIZACIONALES REQUERIDAS", styles['CustomHeading']))
        torres = analysis_data.get("required_towers", [])
        story.append(Paragraph(", ".join(torres), styles['Normal']))
        story.append(Spacer(1, 0.3*inch))
        
        # Equipos recomendados
        story.append(Paragraph("EQUIPOS RECOMENDADOS", styles['CustomHeading']))
        teams_data = []
        teams_data.append(['Equipo', 'Torre', 'Confianza', 'Rol'])
        
        for team in analysis_data.get("team_recommendations", [])[:5]:
            teams_data.append([
                team.get('team_name', ''),
                team.get('tower', ''),
                f"{team.get('confidence_score', 0)*100:.0f}%",
                team.get('recommended_role', '')
            ])
        
        if len(teams_data) > 1:
            teams_table = Table(teams_data, colWidths=[1.5*inch, 1*inch, 0.8*inch, 2*inch])
            teams_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0078D4')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(teams_table)
        story.append(Spacer(1, 0.3*inch))
        
        # Riesgos principales
        story.append(Paragraph("RIESGOS PRINCIPALES", styles['CustomHeading']))
        for risk in analysis_data.get("risks", [])[:3]:
            story.append(Paragraph(
                f"<b>{risk.get('category', '')} ({risk.get('level', '')})</b>",
                styles['Normal']
            ))
            story.append(Paragraph(risk.get('description', ''), styles['Normal']))
            story.append(Paragraph(f"<i>Mitigación: {risk.get('mitigation', '')}</i>", styles['Normal']))
            story.append(Spacer(1, 0.15*inch))
        
        story.append(Spacer(1, 0.2*inch))
        
        # Estimaciones
        story.append(Paragraph("ESTIMACIONES", styles['CustomHeading']))
        story.append(Paragraph(
            f"<b>Timeline:</b> {analysis_data.get('timeline_estimate', 'N/A')}",
            styles['Normal']
        ))
        
        budget = analysis_data.get('budget_estimate', {})
        if budget:
            story.append(Paragraph(
                f"<b>Rango de costo:</b> {budget.get('estimated_cost_range', 'N/A')}",
                styles['Normal']
            ))
            story.append(Paragraph(
                f"<b>Horas estimadas:</b> {budget.get('estimated_hours', 'N/A')}",
                styles['Normal']
            ))
        
        story.append(Spacer(1, 0.3*inch))
        
        # Recomendaciones
        story.append(Paragraph("RECOMENDACIONES", styles['CustomHeading']))
        for rec in analysis_data.get("recommendations", []):
            story.append(Paragraph(f"• {rec}", styles['Normal']))
            story.append(Spacer(1, 0.1*inch))
        
        # Generar PDF
        doc.build(story)
        
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        logging.info(f"✅ PDF generado: {len(pdf_bytes)} bytes")
        return pdf_bytes
        
    except Exception as e:
        logging.error(f"❌ Error generando PDF: {str(e)}")
        return None


def generate_technical_pdf(
    work_item_id: int,
    title: str,
    analysis_data: Dict[str, Any]
) -> Optional[bytes]:
    """
    Genera un PDF técnico detallado
    (Por implementar si se requiere)
    """
    # Similar a generate_executive_pdf pero con más detalles técnicos
    return None
