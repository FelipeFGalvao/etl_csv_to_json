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
