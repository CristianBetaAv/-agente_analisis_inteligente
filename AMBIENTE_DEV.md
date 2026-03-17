# 🔧 Ambiente: DESARROLLO

> **Este proyecto está configurado exclusivamente para desarrollo y testing local**

## 📋 Características del Ambiente

- 🔧 **Configuración local** y de testing
- 📝 **Logs detallados** y modo verbose activado
- ⚙️ **Recursos dedicados** de Azure para DEV
- 🧪 **Testing** sin afectar producción
- 🚀 **Despliegues experimentales** permitidos

---

## ☁️ Recursos de Azure Asociados

| Recurso | Nombre | Propósito |
|---------|--------|-----------|
| **Azure Function** | `function-analyzer-perxia-solver` | Flex Consumption (DEV) |
| **Azure OpenAI** | `oai-agente-perxia-dev` | Modelo: gpt-4o-mini |
| **AI Search** | `search-agente-perxia-dev` | Índice: torres-index |
| **Blob Storage** | `stagenteperxiadev` | Contenedor: analysis-pdfs |
| **Cosmos DB** | `cosmos-agente-perxia-dev` | DB: opportunity-analysis |
| **Key Vault** | `kv-agente-perxia-dev` | Secretos de desarrollo |

**Endpoint de la Function:**
```
https://function-analyzer-perxia-solver-czc0cgf5czfmbjh4.eastus2-01.azurewebsites.net/api/analyze
```

**Function Key:**
```
0sI4xIqLMLMGcdG6btpLCKt7lF9vpROD1w5KDrzAOiE_AzFu5V6zuA==
```

---

## 📁 Archivos de Configuración

### 1. `local.settings.json`
Configuración para ejecución local con Azure Functions Core Tools.

✅ **Ya configurado** con credenciales de desarrollo.

### 2. `.env`
Variables de entorno para scripts y testing.

✅ **Ya configurado** con las mismas credenciales.

### 3. `.gitignore`
⚠️ **Importante**: Los archivos `.env` y `local.settings.json` están excluidos de Git por seguridad.

---

## 🚀 Cómo Usar Este Ambiente

### 1. Ejecución Local

```powershell
# Activar entorno virtual
.venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Iniciar Azure Function localmente
func start
```

La función estará disponible en: `http://localhost:7071/api/analyze`

### 2. Testing con Payload de Ejemplo

```powershell
# Test básico
Invoke-RestMethod -Uri "http://localhost:7071/api/analyze" `
  -Method Post `
  -ContentType "application/json" `
  -Body (Get-Content test_payload.json -Raw)

# Test con datos reales
Invoke-RestMethod -Uri "http://localhost:7071/api/analyze" `
  -Method Post `
  -ContentType "application/json" `
  -Body (Get-Content test_payload_real.json -Raw)
```

### 3. Despliegue a Azure (Testing en Nube)

```bash
# Login
az login

# Desplegar
func azure functionapp publish function-analyzer-perxia-solver
```

---

## 🛠️ Scripts Disponibles

| Script | Propósito |
|--------|-----------|
| `scripts/setup_search_index.py` | Configurar/actualizar índice de AI Search |
| `scripts/test_connections.py` | Validar conectividad con servicios Azure |

---

## 🔐 Seguridad

- ✅ Todas las credenciales están en archivos locales (`.env`, `local.settings.json`)
- ✅ Estos archivos están excluidos de Git mediante `.gitignore`
- ✅ Los recursos de Azure usan sufijo `-dev` para diferenciarlos de PROD
- ⚠️ **NUNCA** subir credenciales a GitHub

---

## 📊 Datos de Testing

### Torres Disponibles (13)
Ver archivo: `data/torres_nuevas.json`

1. CIBERSEGURIDAD
2. PMO
3. DATA
4. IA
5. RPA
6. FULLSTACK
7. QA
8. MOBILE
9. SAP
10. INTEGRACION
11. PORTALES
12. SOPORTE Y MANTENIMIENTO
13. DEVOPS

### Knowledge Base
- `data/KN_TALLAJE_*.json` - Estimaciones de horas por torre
- `data/teams_data.json` - Información de equipos

---

## 🔄 Integración con Power Automate

### Configuración del Flujo (DEV)

1. **Trigger**: "When a row is added" (Dataverse - Opportunity)
2. **Action**: HTTP POST
   - **URL**: Endpoint de la Function (ver arriba)
   - **Method**: POST
   - **Headers**: 
     ```json
     {
       "Content-Type": "application/json",
       "x-functions-key": "0sI4xIqLMLMGcdG6btpLCKt7lF9vpROD1w5KDrzAOiE_AzFu5V6zuA=="
     }
     ```
   - **Body**: Contenido de la oportunidad desde Dynamics 365

3. **Action**: Parse JSON
   - **Schema**: Simplificado (sin required)

4. **Action**: Post Adaptive Card to Teams
   - **Card**: `@{body('Parse_JSON')?['body']?['outputs']?['adaptive_card']}`

---

## 🐛 Debugging

### Logs en Azure
```bash
# Ver logs en tiempo real
func azure functionapp logstream function-analyzer-perxia-solver
```

### Logs Locales
Los logs aparecen en la consola al ejecutar `func start`

### Application Insights
- Monitoreo disponible en Azure Portal
- Buscar por `operation_Id` o `opportunity_id`

---

## 📝 Notas Importantes

- ⚠️ **Este NO es el ambiente de PRODUCCIÓN**
- ✅ Cambios experimentales están permitidos
- ✅ Puedes modificar configuraciones sin temor
- ✅ Los errores NO afectan usuarios finales
- 📁 Para PRODUCCIÓN, usar: `agente_analisis_inteligente_prod`

---

## 🔗 Enlaces Relacionados

- **Repositorio**: `https://github.com/malekai-cyber/agente_analisis_inteligente.git`
- **Documentación Principal**: `README.md`
- **Historial de Cambios**: `HISTORIAL_DESARROLLO.md`
- **Azure Portal**: Buscar recursos con sufijo `-dev`

---

**Fecha de creación**: 6 de Febrero 2026  
**Propósito**: Desarrollo, testing y validación local  
**Ambiente**: DESARROLLO 🔧
