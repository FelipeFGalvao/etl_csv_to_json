import pytest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock
from etl.pipeline import ETL

#-------------------------------------------------------fixtures
@pytest.fixture
def sample_data():
    """Dados de exemplo válidos para testes"""
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
    """Schema padrão para validação"""
    return {
        "TITLE": str, "F_NAME": str, "L_NAME": str, "GENDER": str,
        "MONTH_AND_DATE": str, "DOB": str, "YOB": int, "EMAIL": str,
        "ID1": str, "ID2": str, "ID3": str, "ID4": str, "PHONE": str,
        "EMAIL2": str, "STREET": str, "CITY": str, "STATE": str,
        "COUNTRY": str, "ZIP": str, "LAT": float, "LONG": float,
    }

@pytest.fixture
def required_fields():
    """Campos obrigatórios para validação"""
    return ["F_NAME", "L_NAME", "EMAIL", "PHONE"]

@pytest.fixture
def mixed_valid_invalid_data():
    """Dados mistos com registros válidos e inválidos para testes de lote"""
    return [
        # Registro válido 
        {
            "TITLE": "Mr", "F_NAME": "Carlos", "L_NAME": "Santos", "GENDER": "male",
            "MONTH_AND_DATE": "05-15", "DOB": "1985-05-15", "YOB": "1985", 
            "EMAIL": "carlos@example.com", "ID1": "111", "ID2": "222", "ID3": "333", 
            "ID4": "444", "PHONE": "987654321", "EMAIL2": "carlos2@example.com",
            "STREET": "Rua B", "CITY": "Rio de Janeiro", "STATE": "RJ", 
            "COUNTRY": "Brazil", "ZIP": "11111-111", "LAT": "-22.9068", "LONG": "-43.1729"
        },
        # Registro inválido (YOB como string não numérica) 
        {
            "TITLE": "Mrs", "F_NAME": "Maria", "L_NAME": "Costa", "GENDER": "female",
            "MONTH_AND_DATE": "08-20", "DOB": "1992-08-20", "YOB": "not_a_year", 
            "EMAIL": "maria@example.com", "ID1": "555", "ID2": "666", "ID3": "777", 
            "ID4": "888", "PHONE": "111222333", "EMAIL2": "maria2@example.com",
            "STREET": "Rua C", "CITY": "Belo Horizonte", "STATE": "MG", 
            "COUNTRY": "Brazil", "ZIP": "22222-222", "LAT": "invalid_lat", "LONG": "-43.9378"
        },
        # Registro válido (ano de nascimento com 4 dígitos) 
        {
            "TITLE": "Dr", "F_NAME": "Ana", "L_NAME": "Oliveira", "GENDER": "female",
            "MONTH_AND_DATE": "12-10", "DOB": "1988-12-10", "YOB": "1988", 
            "EMAIL": "ana@example.com", "ID1": "999", "ID2": "000", "ID3": "111", 
            "ID4": "222", "PHONE": "444555666", "EMAIL2": "ana2@example.com",
            "STREET": "Rua D", "CITY": "Salvador", "STATE": "BA", 
            "COUNTRY": "Brazil", "ZIP": "33333-333", "LAT": "-12.9714", "LONG": "-38.5014"
        }
    ]

#-------------------------------------------------------testes ETL pipeline
class TestETLExtract:
    """Testes para a função extract() do pipeline ETL"""
    
    def test_extract_success(self, sample_data):
        """Testa a extração bem-sucedida de um arquivo CSV"""
        csv_content = "\n".join([
            ",".join(sample_data[0].keys()),
            ",".join(str(v) for v in sample_data[0].values())
        ])
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            etl = ETL("dummy.csv", "dummy.json")
            result = etl.extract()
            assert len(result) == 1
            assert result[0]["F_NAME"] == "João"
    
    def test_extract_file_not_found(self):
        """Testa o tratamento de erro pra caso o arquivo noa exista"""
        etl = ETL('data/input/arquivo_que_nao_existe.csv', 'fake.json')
        with pytest.raises(FileNotFoundError):
            etl.extract()
    
    def test_extract_empty_csv(self):
        """Testa a extração de um arquivo CSV vazio"""
        with patch("builtins.open", mock_open(read_data="")):
            etl = ETL("empty.csv", "dummy.json")
            result = etl.extract()
            assert result == []
    
    def test_extract_csv_with_only_headers(self):
        """Testa a extração de CSV com apenas cabeçalhos"""
        csv_content = "F_NAME,L_NAME,EMAIL"
        
        with patch("builtins.open", mock_open(read_data=csv_content)):
            etl = ETL("headers_only.csv", "dummy.json")
            result = etl.extract()
            assert result == []
