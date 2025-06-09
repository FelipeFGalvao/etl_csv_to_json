import os
import logging
from typing import Dict, Any, List


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def validate_csv_exists(file_path) -> None:
    """
    Valida se o arquivo CSV existe no caminho informado.
    """
    if not os.path.isfile(file_path):
        logger.error(f"Arquivo CSV não encontrado: {file_path}")
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {file_path}")
    
    logger.info(f"Arquivo CSV validado com sucesso: {file_path}")


def validate_data(data: List[Dict[str, Any]]) -> bool: #funcao para validar os dados
    if not data:
        logger.warning("O arquivo CSV está vazio!")
        return False
    
    logger.info(f"Dados validados: {len(data)} registros encontrados")
    return True

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