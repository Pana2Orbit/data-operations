venv:
	if command -v python3 >/dev/null 2>&1; then \
		python3 -m venv .venv; \
	else \
		python -m venv .venv; \
	fi

activate:
	@if [ "$$(uname -s)" = "Darwin" ]; then \
		source .venv/bin/activate; \
	elif [ "$$(uname -s)" = "Linux" ]; then \
		source .venv/bin/activate; \
	else \
		source .venv/Scripts/activate || (echo "Bash activation failed. Try manually:" && echo "  CMD: .venv\\Scripts\\activate.bat" && echo "  PowerShell: .venv\\Scripts\\Activate.ps1"); \
	fi
