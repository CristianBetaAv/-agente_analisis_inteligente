# 🤖 Agente de Análisis Inteligente

![Ambiente](https://img.shields.io/badge/Ambiente-DESARROLLO-orange?style=for-the-badge)
![Azure](https://img.shields.io/badge/Azure-Functions-blue?style=for-the-badge&logo=microsoft-azure)
![Python](https://img.shields.io/badge/Python-3.12-green?style=for-the-badge&logo=python)
![Status](https://img.shields.io/badge/Status-Funcionando-success?style=for-the-badge)

> **⚠️ AMBIENTE DE DESARROLLO**  
> Este proyecto está configurado exclusivamente para **desarrollo y testing local**.  
> Para producción, consultar el repositorio: `agente_analisis_inteligente_prod`

Sistema de análisis automático de oportunidades comerciales usando IA (gpt-4o-mini).

## � Inicio Rápido

```powershell
# 1. Activar entorno virtual
.venv\Scripts\activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar localmente
func start

# 4. Test
Invoke-RestMethod -Uri "http://localhost:7071/api/analyze" `
  -Method Post `
  -ContentType "application/json" `
  -Body (Get-Content test_payload.json -Raw)
```

> 📖 **Documentación adicional**: Ver [AMBIENTE_DEV.md](AMBIENTE_DEV.md) para configuración detallada

---

## 📋 Descripción

Este proyecto implementa una Azure Function que recibe oportunidades desde **Microsoft Dynamics 365** (vía Power Automate) y genera análisis inteligentes que incluyen:

- ✅ **Resumen ejecutivo** del requerimiento
- ✅ **Recomendación de equipos/torres** según las habilidades requeridas
- ✅ **Evaluación de riesgos** con mitigaciones
- ✅ **Estimación de esfuerzo** y timeline
- ✅ **Adaptive Card** para Microsoft Teams
- ✅ **PDF** con el análisis completo

## 🏗️ Recursos de Azure (Desarrollo)

- **Azure Function**: `function-analyzer-perxia-solver` (Flex Consumption)
- **Azure OpenAI**: `oai-agente-perxia-dev` (gpt-4o-mini)
- **Azure AI Search**: `search-agente-perxia-dev` (torres-index)
- **Azure Blob Storage**: `stagenteperxiadev` (analysis-pdfs)
- **Cosmos DB**: `cosmos-agente-perxia-dev` (opportunity-analysis)
- **Key Vault**: `kv-agente-perxia-dev`

## 🏗️ Arquitectura

```
┌─────────────────┐     HTTP POST     ┌──────────────────────┐
│  Power Automate │ ────────────────► │  Azure Function      │
│  (Dataverse     │                   │  AnalyzeOpportunity  │
│   Trigger)      │                   └──────────┬───────────┘
└─────────────────┘                              │
                                                 ▼
                               ┌─────────────────────────────────┐
                               │     OpportunityOrchestrator     │
                               └─────────────────────────────────┘
                                        │
         ┌──────────────────────────────┼──────────────────────────────┐
         │                              │                              │
         ▼                              ▼                              ▼
┌─────────────────┐          ┌──────────────────┐          ┌──────────────────┐
│  Azure OpenAI   │          │  Azure AI Search │          │  Azure Blob      │
│  (DeepSeek-R1)  │          │  (Teams Index)   │          │  Storage (PDFs)  │
└─────────────────┘          └──────────────────┘          └──────────────────┘
         │                              │                              │
         └──────────────────────────────┼──────────────────────────────┘
                                        │
                                        ▼
                               ┌─────────────────────────────────┐
                               │         Response JSON           │
                               │  • Analysis                     │
                               │  • Adaptive Card (Teams)        │
                               │  • PDF URL                      │
                               └─────────────────────────────────┘
```

## 📁 Estructura del Proyecto

```
agente_analisis_inteligente/
├── AnalyzeOpportunity/          # Azure Function principal
│   ├── __init__.py              # Handler HTTP
│   └── function.json            # Configuración del trigger
├── shared/
│   ├── core/
│   │   └── orchestrator.py      # Orquestador principal
│   ├── models/
│   │   ├── opportunity.py       # Modelo de oportunidad (Pydantic)
│   │   └── analysis.py          # Modelos de análisis
│   ├── services/
│   │   ├── openai_service.py    # Cliente Azure OpenAI
│   │   ├── search_service.py    # Cliente Azure AI Search
│   │   ├── blob_storage_service.py  # Cliente Blob Storage
│   │   └── cosmos_service.py    # Cliente Cosmos DB (opcional)
│   └── generators/
│       ├── adaptive_card.py     # Generador de Adaptive Cards
│       └── pdf_generator.py     # Generador de PDFs
├── data/
│   └── teams_data.json          # Datos de equipos/torres
├── host.json                    # Configuración de Azure Functions
├── requirements.txt             # Dependencias Python
├── local.settings.json.example  # Ejemplo de configuración local
└── README.md
```

## ⚙️ Configuración

### 1. Variables de Entorno (Desarrollo)

**Opción 1: Usar local.settings.json (ya configurado)**

El archivo `local.settings.json` ya contiene las credenciales de desarrollo:

```json
{
  "Values": {
    "AZURE_OPENAI_ENDPOINT": "https://oai-agente-perxia-dev.openai.azure.com/",
    "AZURE_OPENAI_DEPLOYMENT_NAME": "gpt-4o-mini",
    "AZURE_SEARCH_ENDPOINT": "https://search-agente-perxia-dev.search.windows.net",
    "AZURE_SEARCH_INDEX_TEAMS": "torres-index",
    "AZURE_STORAGE_CONTAINER_NAME": "analysis-pdfs",
    "COSMOS_DATABASE_NAME": "opportunity-analysis",
    "COSMOS_CONTAINER_NAME": "analysis-records"
  }
}
```

**Opción 2: Usar .env (también configurado)**

El archivo `.env` está configurado con las mismas credenciales para desarrollo local.

> ⚠️ **Importante**: Estos archivos contienen credenciales de desarrollo. NO subir a Git.
```

### 2. Azure AI Search Index

El índice de torres ya está configurado en el ambiente de desarrollo.

Para regenerar o actualizar:

```bash
python scripts/setup_search_index.py
```

### 3. Power Automate

Configura un flujo en Power Automate:

1. **Trigger**: "When a row is added" (Dataverse - Opportunity table)
2. **Action**: HTTP POST a tu Azure Function
3. **Body**: El contenido de la oportunidad

## 🚀 Ejecución Local (Desarrollo)

### 1. Instalar Dependencias

```bash
# Activar entorno virtual
.venv\Scripts\activate

# Instalar paquetes
pip install -r requirements.txt
```

### 2. Ejecutar Azure Function Localmente

```bash
# Iniciar Function App
func start
```

La función estará disponible en: `http://localhost:7071/api/analyze`

### 3. Testing con Payload de Ejemplo

```bash
# Usar el payload de prueba
Invoke-RestMethod -Uri "http://localhost:7071/api/analyze" `
  -Method Post `
  -ContentType "application/json" `
  -Body (Get-Content test_payload.json)
```

## 📦 Despliegue a Azure (Desarrollo)

> ⚠️ **Solo para testing en Azure**. Para producción usar el ambiente PROD.

```bash
# Login en Azure
az login

# Desplegar a Function App de desarrollo
func azure functionapp publish function-analyzer-perxia-solver
```

## 📨 Uso

### Endpoints Disponibles

**Local (Desarrollo):**
```
POST http://localhost:7071/api/analyze
```

**Azure (Testing DEV):**
```
POST https://function-analyzer-perxia-solver-czc0cgf5czfmbjh4.eastus2-01.azurewebsites.net/api/analyze
Authorization: 0sI4xIqLMLMGcdG6btpLCKt7lF9vpROD1w5KDrzAOiE_AzFu5V6zuA==
```

### Payload de Ejemplo

```json
{
  "opportunityid": "2f1511d1-0b08-42bc-aeea-62f0f539194b",
  "name": "Implementación de Sistema de IA",
  "description": "El cliente requiere un sistema de inteligencia artificial...",
  "cr807_descripciondelrequerimientofuncional": "Se necesita desarrollar...",
  "estimatedclosedate": "2026-06-30",
  "estimatedvalue": 150000,
  "statecode": 0,
  "SdkMessage": "Create"
}
```

### Respuesta

```json
{
  "success": true,
  "opportunity_id": "2f1511d1-0b08-42bc-aeea-62f0f539194b",
  "opportunity_name": "Implementación de Sistema de IA",
  "analysis": {
    "executive_summary": "...",
    "required_towers": ["Torre IA", "Torre DATA", "Torre FULLSTACK"],
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

## 🏢 Torres Disponibles

| Torre | Especialidad |
|-------|-------------|
| Torre IA | Machine Learning, NLP, IA Generativa |
| Torre DATA | Data Engineering, BI, Analytics |
| Torre CIBERSEGURIDAD | Security, SOC, Compliance |
| Torre RPA | Automatización, Bots, Workflows |
| Torre FULLSTACK | Web Development, APIs, Microservices |
| Torre QA | Testing, Quality Assurance |
| Torre PMO | Project Management, Agile |
| Torre MOBILE | iOS, Android, React Native |
| Torre SAP | SAP ERP, S/4HANA, ABAP |
| Torre INTEGRACION | APIs, ESB, Middleware |
| Torre PORTALES | CMS, SharePoint, Intranet |
| Torre SOPORTE Y MANTENIMIENTO | IT Support, ITIL |
| Torre DEVOPS | CI/CD, Kubernetes, IaC |

## 🧠 Modelo de IA

Este proyecto utiliza **GPT-4o-mini** desplegado en Azure OpenAI (ambiente DEV):

- Deployment: `gpt-4o-mini`
- Endpoint: `oai-agente-perxia-dev`
- API Version: `2024-10-21`
- Optimizado para análisis técnico
- Soporte para español e inglés

## � Enlaces Útiles

- **Repositorio GitHub**: `https://github.com/malekai-cyber/agente_analisis_inteligente.git`
- 📖 [Configuración de Ambiente](AMBIENTE_DEV.md)
- 📝 [Historial de Desarrollo](HISTORIAL_DESARROLLO.md)

## 📝 Notas de Desarrollo

- Todos los recursos apuntan a servicios de **desarrollo** (`-dev` suffix)
- Las credenciales están en `local.settings.json` y `.env` (NO subir a Git)
- Para pruebas E2E, usar `test_payload.json` y `test_payload_real.json`
- Los PDFs generados se almacenan en `stagenteperxiadev/analysis-pdfs`
- Los análisis se guardan en Cosmos DB: `cosmos-agente-perxia-dev`

## 🚨 Importante

### Archivos que NO deben subirse a Git
```
.env
local.settings.json
*.log
__pycache__/
.venv/
```

> **Nota**: El `.gitignore` ya está configurado para proteger estos archivos

## 📄 Licencia

Uso interno - Todos los derechos reservados.

## 👥 Contribuidores

- Desarrollado por el equipo de IA

---

**Ambiente**: DESARROLLO 🔧  
**Última actualización**: 6 de Febrero 2026
