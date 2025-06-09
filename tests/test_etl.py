import json
import pytest
from etl.pipeline import ETL

# teste para funcao de limpeza de espaços em branco, nesse caso
def test_transform_remove_spaces():
    etl = ETL('fake.csv', 'fake.json')

    entrada = [{' Nome ': ' João ', ' Idade ': ' 25 '}]
    esperado = [{'Nome': 'João', 'Idade': '25'}]

    resultado = etl.clean(entrada)

    assert resultado == esperado

# o teste da função de extração pra caso de arquivo inexistente
def test_extract_file_not_found():
    etl = ETL('data/input/arquivo_que_nao_existe.csv', 'fake.json')

    with pytest.raises(FileNotFoundError):
        etl.extract()

# teste da função de carregamento
def test_load_creates_json(tmp_path):
    # Dados simulados
    dados = [{'Nome': 'João', 'Idade': '25'}]

    # Cria caminho temporário para o JSON
    json_path = tmp_path / "output.json"

    etl = ETL('fake.csv', json_path)
    etl.load(dados)

    # Valida se o arquivo foi criado
    assert json_path.exists()

    # Valida conteúdo
    with open(json_path, 'r', encoding='utf-8') as f:
        conteudo = json.load(f)

    assert conteudo == dados
