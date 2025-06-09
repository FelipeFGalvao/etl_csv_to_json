import json
import pytest
from unittest.mock import patch, mock_open
from etl.pipeline import ETL
from etl.utils import validate_schema

# Fixtures para reutilização nos testes
@pytest.fixture
def sample_data():
    return [{
        "TITLE": "Miss",
        "F_NAME": "João",
        "L_NAME": "Silva",
        "GENDER": "male",
        "MONTH_AND_DATE": "01-01",
        "DOB": "1990-01-01",
        "YOB": "1990",
        "EMAIL": "joao@example.com",
        "ID1": "123",
        "ID2": "456",
        "ID3": "789",
        "ID4": "012",
        "PHONE": "123456789",
        "EMAIL2": "joao2@example.com",
        "STREET": "Rua A",
        "CITY": "São Paulo",
        "STATE": "SP",
        "COUNTRY": "Brazil",
        "ZIP": "00000-000",
        "LAT": "-23.5505",
        "LONG": "-46.6333"
    }]

@pytest.fixture
def sample_schema():
    return {
        "TITLE": str,
        "F_NAME": str,
        "L_NAME": str,
        "GENDER": str,
        "MONTH_AND_DATE": str,
        "DOB": str,
        "YOB": int,
        "EMAIL": str,
        "ID1": str,
        "ID2": str,
        "ID3": str,
        "ID4": str,
        "PHONE": str,
        "EMAIL2": str,
        "STREET": str,
        "CITY": str,
        "STATE": str,
        "COUNTRY": str,
        "ZIP": str,
        "LAT": float,
        "LONG": float
    }

# Testes para a função extract()
def test_extract_success(sample_data):
    """Testa a extração bem-sucedida de um arquivo CSV"""
    csv_content = "\n".join([
        ",".join(sample_data[0].keys()),
        ",".join(sample_data[0].values())
    ])
    
    with patch("builtins.open", mock_open(read_data=csv_content)):
        etl = ETL("dummy.csv", "dummy.json")
        result = etl.extract()
        assert len(result) == 1
        assert result[0]["F_NAME"] == "João"

def test_extract_file_not_found():
    """Testa o tratamento de erro quando o arquivo não existe"""
    etl = ETL('data/input/arquivo_que_nao_existe.csv', 'fake.json')
    with pytest.raises(FileNotFoundError):
        etl.extract()

# Testes para a função transform()
def test_transform_remove_spaces():
    """Testa a remoção de espaços em branco nos nomes dos campos e valores"""
    etl = ETL('fake.csv', 'fake.json')
    entrada = [{' Nome ': ' João ', ' Idade ': ' 25 '}]
    esperado = [{'Nome': 'João', 'Idade': '25'}]
    resultado = etl.transform(entrada)
    assert resultado == esperado

def test_transform_empty_data():
    """Testa o comportamento com dados vazios"""
    etl = ETL('fake.csv', 'fake.json')
    with pytest.raises(ValueError, match="Nenhum dado extraído"):
        etl.transform([])

def test_transform_schema_validation(sample_data, sample_schema):
    """Testa a validação do schema durante a transformação"""
    etl = ETL('fake.csv', 'fake.json')
    transformed = etl.transform(sample_data)
    assert len(transformed) == 1
    assert validate_schema(transformed[0], sample_schema)

def test_transform_invalid_data():
    """Testa o comportamento com dados inválidos"""
    etl = ETL('fake.csv', 'fake.json')
    invalid_data = [{"INVALID_FIELD": "value"}]
    result = etl.transform(invalid_data)
    assert len(result) == 0  # Deve filtrar registros inválidos

# Testes para a função load()
def test_load_creates_json(tmp_path, sample_data):
    """Testa a criação correta do arquivo JSON"""
    json_path = tmp_path / "output.json"
    etl = ETL('fake.csv', json_path)
    etl.load(sample_data)
    
    assert json_path.exists()
    
    with open(json_path, 'r', encoding='utf-8') as f:
        content = json.load(f)
    
    assert content == sample_data

def test_load_creates_directory(tmp_path, sample_data):
    """Testa a criação automática do diretório de saída"""
    json_path = tmp_path / "new_dir" / "output.json"
    etl = ETL('fake.csv', json_path)
    etl.load(sample_data)
    assert json_path.exists()

# Testes para o fluxo completo ETL
def test_run_complete_flow(tmp_path, sample_data):
    """Testa o fluxo ETL completo"""
    # Mock para a extração
    csv_content = "\n".join([
        ",".join(sample_data[0].keys()),
        ",".join(sample_data[0].values())
    ])
    
    json_path = tmp_path / "output.json"
    
    with patch("builtins.open", mock_open(read_data=csv_content)):
        etl = ETL('dummy.csv', json_path)
        etl.run()
    
    assert json_path.exists()
    with open(json_path, 'r', encoding='utf-8') as f:
        content = json.load(f)
    assert len(content) == 1

# Testes para a validação de schema
def test_validate_schema_valid(sample_data, sample_schema):
    """Testa a validação de schema com dados válidos"""
    assert validate_schema(sample_data[0], sample_schema) == True

def test_validate_schema_missing_field(sample_data, sample_schema):
    """Testa a validação com campos faltantes"""
    invalid_data = sample_data[0].copy()
    del invalid_data["F_NAME"]
    assert validate_schema(invalid_data, sample_schema) == False

def test_validate_schema_wrong_type(sample_data, sample_schema):
    """Testa a validação com tipos incorretos"""
    invalid_data = sample_data[0].copy()
    invalid_data["YOB"] = "not_a_number"
    assert validate_schema(invalid_data, sample_schema) == False

def test_validate_schema_empty_data():
    """Testa a validação com dados vazios"""
    assert validate_schema({}, {}) == True  # Schema vazio aceita dados vazios