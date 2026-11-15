# freshcart-pipelines

Data Engineering assignment: "From Git to Gold"

## Parts
- **Part A** – Git and CI/CD (`tests/`, `.github/workflows/`)
- **Part B** – FreshCart ELT on Snowflake (`elt/`)
- **Part C** – ML pipeline with Mage (`iris_project/`)
- **Part D** – Food-order stream with RabbitMQ (`stream/`)

## Setup
```bash
pip install -r requirements.txt
```

## Run the Mage pipeline
```bash
mage run iris_project iris_classifier --test
```

## Run CI tests locally
```bash
python -m pytest tests/
```
