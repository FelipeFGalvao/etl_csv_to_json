import os
import logging

def validate_csv_exists(file_path):
    """
    Valida se o arquivo CSV existe no caminho informado.
    """
    if not os.path.isfile(file_path):
        logging.error(f"Arquivo CSV não encontrado: {file_path}")
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {file_path}")

def validate_data(data):
    """
    Validação simples: verifica se os dados não estão vazios.
    """
    if not data:
        logging.warning("O arquivo CSV está vazio!")
def validate_schema(record: dict, schema: dict) -> bool: #funcao para validar o schema record (dict): Linha do CSV já convertida para dicionário.schema (dict): Dicionário com os campos esperados e seus tipos. retorna true  or false

    for key, expected_type in schema.items():
        if key not in record:
            print(f"[ERRO] Campo {key} não encontrado no registro.")
            return False
        try:
            # Tentativa de conversão para o tipo esperado
            if expected_type == int:
                int(record[key])
            elif expected_type == float:
                float(record[key])
            elif expected_type == str:
                str(record[key])
            else:
                # Caso vá suportar outros tipos depois
                pass
        except (ValueError, TypeError):
            print(f"[ERRO] Campo {key} com valor inválido: {record[key]}")
            return False
    return True