# 📋 Historial de Desarrollo - Agente de Análisis Inteligente

> **🔧 AMBIENTE: DESARROLLO**  
> Este documento registra los cambios del proyecto en el ambiente de desarrollo

## 🎯 Resumen del Proyecto

Este proyecto fue creado como una reestructuración de **PerxiaSolverApp**, eliminando la dependencia de Azure DevOps y enfocándose en recibir oportunidades de **Dynamics 365** vía **Power Automate**.

**Repositorio**: `https://github.com/malekai-cyber/agente_analisis_inteligente.git`

---

## 📅 Sesión de Desarrollo - 3 de Febrero 2026

### Contexto Inicial

El proyecto original (PerxiaSolverApp) era una Azure Function que:
- Recibía webhooks de Azure DevOps cuando se creaba un Work Item
- Analizaba propuestas técnicas con IA (DeepSeek-R1)
- Generaba análisis con equipos recomendados

### Cambio de Requerimientos

El usuario necesitaba:
1. **Eliminar dependencia de Azure DevOps** - Ya no se usará
2. **Integrar con Dynamics 365** - Recibir oportunidades comerciales
3. **Flujo via Power Automate** - HTTP POST con el body de la oportunidad
4. **Nuevo repositorio** - `https://github.com/malekai-cyber/agente_analisis_inteligente.git`

---

## 🔄 Cambios Realizados

### 1. Nueva Estructura de Proyecto

```
agente_analisis_inteligente/
├── AnalyzeOpportunity/          # Azure Function (antes ProcessProposal)
│   ├── __init__.py              # Handler HTTP POST /api/analyze
│   └── function.json
├── shared/
│   ├── core/
│   │   └── orchestrator.py      # OpportunityOrchestrator
│   ├── models/
│   │   ├── opportunity.py       # OpportunityPayload (Pydantic)
│   │   └── analysis.py          # Modelos de análisis
│   ├── services/
│   │   ├── openai_service.py    # DeepSeek-R1 (analyze_opportunity)
│   │   ├── search_service.py    # Azure AI Search
│   │   ├── blob_storage_service.py
│   │   └── cosmos_service.py
│   └── generators/
│       ├── adaptive_card.py     # generate_opportunity_card()
│       └── pdf_generator.py
├── data/
│   └── teams_data.json          # 13 torres organizacionales
└── [configuración]
```

### 2. Modelos de Datos

**Antes (Azure DevOps):**
```python
class AzureDevOpsPayload(BaseModel):
    resource: WorkItemResource
    eventType: str
    ...
```

**Ahora (Dynamics 365):**
```python
class OpportunityPayload(BaseModel):
    opportunityid: str
    name: str
    description: Optional[str]
    cr807_descripciondelrequerimientofuncional: Optional[str]
    estimatedclosedate: Optional[str]
    estimatedvalue: Optional[float]
    statecode: Optional[int]
    SdkMessage: Optional[str]  # Create, Update, Delete
    ...
```

### 3. Orquestador Simplificado

El nuevo `OpportunityOrchestrator` tiene un flujo de 9 pasos:

1. **Validar payload** - Parsear con Pydantic
2. **Preparar texto** - `format_for_analysis()`
3. **Buscar equipos** - Azure AI Search
4. **Análisis IA** - DeepSeek-R1
5. **Procesar torres** - Enriquecer con datos reales
6. **Guardar Cosmos** - Persistencia (opcional)
7. **Generar PDF** - Subir a Blob Storage
8. **Generar Adaptive Card** - Para Teams
9. **Retornar respuesta** - JSON estructurado

### 4. Endpoint API

**Antes:**
```
POST /api/ProcessProposal
```

**Ahora:**
```
POST /api/analyze
```

### 5. Torres Organizacionales

Se mantienen las 13 torres del proyecto anterior:

| ID | Torre | Especialidad |
|----|-------|-------------|
| 1 | IA | Machine Learning, NLP, IA Generativa |
| 2 | DATA | Data Engineering, BI, Analytics |
| 3 | CIBERSEGURIDAD | Security, SOC, Compliance |
| 4 | RPA | Automatización, Bots, Workflows |
| 5 | FULLSTACK | Web Development, APIs |
| 6 | QA | Testing, Quality Assurance |
| 7 | PMO | Project Management, Agile |
| 8 | MOBILE | iOS, Android, React Native |
| 9 | SAP | SAP ERP, S/4HANA, ABAP |
| 10 | INTEGRACION | APIs, ESB, Middleware |
| 11 | PORTALES | CMS, SharePoint, Intranet |
| 12 | SOPORTE Y MANTENIMIENTO | IT Support, ITIL |
| 13 | DEVOPS | CI/CD, Kubernetes, IaC |

---

## 🛠️ Configuración Local

### Variables de Entorno Requeridas

```json
{
  "AZURE_OPENAI_ENDPOINT": "https://...",
  "AZURE_OPENAI_KEY": "...",
  "AZURE_OPENAI_DEPLOYMENT_NAME": "DeepSeek-R1",
  
  "AZURE_SEARCH_ENDPOINT": "https://...",
  "AZURE_SEARCH_KEY": "...",
  "AZURE_SEARCH_INDEX_TEAMS": "teams-knowledge-base",
  
  "AZURE_STORAGE_CONNECTION_STRING": "...",
  "AZURE_STORAGE_CONTAINER_NAME": "analysis-pdfs"
}
```

### Ejecutar Localmente

```powershell
cd c:\Users\DanielGarca\Desktop\agente_analisis_inteligente
.\.venv\Scripts\Activate.ps1
func start
```

### Probar con Payload

```powershell
$payload = Get-Content "test_payload.json" -Raw
Invoke-RestMethod -Uri "http://localhost:7071/api/analyze" -Method POST -Body $payload -ContentType "application/json"
```

---

## 📡 Integración con Power Automate

### Flujo Recomendado

1. **Trigger**: "When a row is added, modified or deleted" (Dataverse)
   - Table: Opportunity
   - Scope: Organization
   - Change type: Added

2. **Action**: HTTP POST
   - URI: `https://tu-function-app.azurewebsites.net/api/analyze`
   - Method: POST
   - Body: `@{triggerOutputs()?['body']}`

3. **Action**: Parse JSON (respuesta)

4. **Action**: Post adaptive card to Teams
   - Usar `outputs.adaptive_card` de la respuesta

---

## 📦 Payload de Ejemplo

```json
{
  "opportunityid": "2f1511d1-0b08-42bc-aeea-62f0f539194b",
  "name": "Implementación de Sistema de IA",
  "description": "Proyecto de inteligencia artificial...",
  "cr807_descripciondelrequerimientofuncional": "El cliente requiere...",
  "estimatedclosedate": "2026-06-30",
  "estimatedvalue": 150000,
  "statecode": 0,
  "SdkMessage": "Create"
}
```

---

## 📤 Respuesta de Ejemplo

```json
{
  "success": true,
  "opportunity_id": "...",
  "opportunity_name": "...",
  "analysis": {
    "executive_summary": "...",
    "required_towers": ["Torre IA", "Torre DATA"],
    "team_recommendations": [...],
    "overall_risk_level": "Medio",
    "timeline_estimate": {...},
    "effort_estimate": {...}
  },
  "outputs": {
    "adaptive_card": {...},
    "pdf_url": "https://storage.blob.../analysis.pdf"
  }
}
```

---

## 🔗 Referencias

- **Repositorio anterior**: PerxiaSolverApp
- **Nuevo repositorio**: https://github.com/malekai-cyber/agente_analisis_inteligente.git

---

## ✅ Estado del Proyecto

## ✅ Estado del Proyecto (Actualizado: 6 Feb 2026)

### Completado
- [x] Estructura de carpetas creada
- [x] Modelos de datos (Pydantic)
- [x] Servicios (OpenAI, Search, Blob, Cosmos)
- [x] Orquestador principal
- [x] Generadores (Adaptive Card, PDF)
- [x] Azure Function configurada y desplegada
- [x] Git inicializado y push a GitHub
- [x] Entorno virtual creado
- [x] Dependencias instaladas
- [x] `local.settings.json` configurado con credenciales DEV
- [x] `.env` configurado para desarrollo
- [x] Probado localmente (exitoso)
- [x] Desplegado a Azure (function-analyzer-perxia-solver)
- [x] Power Automate configurado y funcionando
- [x] Integración con Teams (Adaptive Cards)
- [x] 13 torres actualizadas con líderes y skills
- [x] Estructura de respuesta corregida (outputs)
- [x] Bug de Parse JSON en Power Automate resuelto
- [x] Ambiente separado (DEV vs PROD)

### Documentación Actualizada
- [x] README.md (con recursos DEV)
- [x] AMBIENTE_DEV.md (configuración completa)
- [x] HISTORIAL_DESARROLLO.md (este archivo)

---

**Última actualización**: 6 de Febrero 2026  
**Ambiente**: DESARROLLO 🔧  
**Estado**: ✅ Funcionando completamente

---

*Última actualización: 3 de Febrero 2026*
