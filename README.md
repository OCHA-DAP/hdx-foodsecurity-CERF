# hdx-foodsecurity-CERF
hdx-foodsecurity-CERF is a python repository generating structured information regarding the food security situation of multiple countries to be used by the UN CERF. View the readme.md file for more information.

## Development Setup

### Quick Start

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e.

# Set up pre-commit
pre-commit install

# Optional: Run hooks against all files
pre-commit run --all-files
```

### Development Workflow

- Write code and commit as usual - pre-commit hooks will run automatically
- For manual code formatting/linting:
  ```bash
  ruff check .    # Check for issues
  ruff format .   # Format code
  ```
- To skip pre-commit hooks (not recommended):
  ```bash
  git commit -m "message" --no-verify
  ```

### VS Code Settings

Add to your `.vscode/settings.json`:
```json
{
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.fixAll": true
    }
}
```
