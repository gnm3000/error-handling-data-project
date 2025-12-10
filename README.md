# Polarspipe — Agentic CLI + Polars ETL

Instrucción natural → plan → código Polars → ejecución aislada en E2B → artefacto final. El núcleo ETL sigue vivo en `polarspipe/ingestion` y ahora puede usarse como tool del agente.

## Arquitectura
- ETL Polars reusable: `polarspipe/ingestion/` + `polarspipe/pipeline.py`.
- Agente LangGraph + OpenAI SDK + E2B: `polarspipe/agent/` (ver `README_AGENT.md` para el grafo).
- CLI agentic: `polarspipe/cli.py` (`polarspipe` como entrypoint).
- Datos de ejemplo en `generation-data/` y scripts de calidad en `scripts/`.

```
polarspipe/
├── polarspipe/
│   ├── agent/        # grafo LangGraph, tools E2B, prompts
│   ├── cli.py        # CLI agentic
│   ├── ingestion/    # reader/transformer/validator/writer
│   ├── pipeline.py   # ETL base reutilizable
│   └── main.py
└── tests/...
```

## Uso rápido
1) Instala dependencias: `uv sync --extra dev`  
2) Crea un `.env` (basado en `.env.example`) con `OPENAI_API_KEY=...` y `E2B_API_KEY=...` (opcionalmente `OPENAI_MODEL=gpt-4.1`). Opcional: `LANGSMITH_API_KEY`, `LANGSMITH_PROJECT`, `LANGSMITH_TRACING=true` para trazas. El CLI carga `.env` automáticamente sin pisar variables ya exportadas.  
3) Corre el agente:  
```bash
polarspipe run "My file `data.json`: extract columns id,name, filter where id!='', save to outputs/result.parquet"
```
El CLI imprime plan, código generado, logs del sandbox y guarda el artefacto si está disponible.

## Flujo agentico
1. Parse: el nodo `parse` normaliza la instrucción a JSON (input, columns, filters, output).  
2. Plan: el nodo `plan` produce pasos concretos.  
3. Codegen: `generate_polars_code` arma un script determinista.  
4. Exec: `execute_in_e2b` lanza un sandbox E2B, instala Polars, corre el script y retorna stdout/stderr + artefacto.  

## Desarrollo y pruebas
- Pipeline local sin agente: `make run` (usa `polarspipe/pipeline.py`).  
- Calidad: `./scripts/run_quality.sh` (o `./scripts/run_quality.sh check`).  
- Benchmarks y humo en `tests/`.  
