## Desarrollo

1. Crea el entorno y dependencias: `python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"`
2. Ejecuta el pipeline de calidad (formatea, lint, tipos, seguridad y tests):
   - Auto-fix: `./scripts/run_quality.sh`
   - Solo verificar sin modificar: `./scripts/run_quality.sh check`
