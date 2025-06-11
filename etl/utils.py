import os
import logging
from typing import Dict, Any, List
from pathlib import Path



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def validate_csv_exists(file_path: Union[str, Path]) -> None:
    try:
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            error_msg = f"Arquivo CSV não encontrado: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        if not file_path_obj.is_file():
            error_msg = f"Caminho não aponta para um arquivo: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        # Verificação adicional da extensão
        if file_path_obj.suffix.lower() not in ['.csv', '.txt']:
            logger.warning(f"Arquivo não tem extensão CSV: {file_path}")
        
        logger.info(f"✅ Arquivo CSV validado com sucesso: {file_path}")
        
    except (TypeError, ValueError) as e:
        error_msg = f"Caminho inválido fornecido: {file_path} - {str(e)}"
        logger.error(error_msg)
        raise TypeError(error_msg) from e


def validate_data(data: List[Dict[str, Any]]) -> bool: #funcao para validar os dados devolvendo de forma booleana
    if not isinstance(data, list):
            logger.error(f"Dados devem ser uma lista, recebido: {type(data)}")
            return False
        
    if not data:
        logger.warning("⚠️ O arquivo CSV está vazio ou não contém dados válidos!")
        return False
        
        # Verificação adicional: se todos os elementos são dicionários
    non_dict_count = sum(1 for item in data if not isinstance(item, dict))
    if non_dict_count > 0:
            logger.warning(f"⚠️ {non_dict_count} registros não são dicionários válidos")
        
    logger.info(f"✅ Dados validados: {len(data)} registros encontrados")
    return True

def validate_schema(record: Dict[str, Any], schema: Dict[str, type]) -> bool: #funcao para validar o schema de um dicionario

    if not isinstance(record, dict):
        logger.error(f"Registro deve ser um dicionário, recebido: {type(record)}")
        return False
    
    if not isinstance(schema, dict):
        logger.error(f"Schema deve ser um dicionário, recebido: {type(schema)}")
        return False
    
    errors = []
    
    for key, expected_type in schema.items():
        if key not in record:
            errors.append(f"Campo '{key}' não encontrado")
            continue
        
        try:
            value = record[key]
            
            # Permitir valores None/vazios para campos opcionais
            if value is None or (isinstance(value, str) and value.strip() == ''):
                logger.debug(f"Campo '{key}' está vazio/None")
                continue
            
            # Validação por tipo
            if expected_type == int:
                # Tenta converter para int
                converted_value = int(float(str(value)))  # Suporta "30.0" -> 30
                if str(converted_value) != str(value).split('.')[0]:
                    logger.debug(f"Valor convertido para int: '{value}' -> {converted_value}")
                    
            elif expected_type == float:
                float(value)
                
            elif expected_type == str:
                str(value)
                
            else:
                # Tipos não implementados especificamente
                logger.debug(f"Tipo {expected_type.__name__} validado genericamente para campo '{key}'")
                
        except (ValueError, TypeError, AttributeError) as e:
            error_detail = f"Campo '{key}' com valor inválido: '{value}' (esperado: {expected_type.__name__})"
            errors.append(error_detail)
            logger.debug(f"Erro de validação: {error_detail} - {str(e)}")
    
    if errors:
        logger.error(f"Registro inválido - {len(errors)} erro(s): {'; '.join(errors)}")
        return False
    
    logger.debug(f"✅ Registro validado com sucesso: {len(record)} campos")
    return True

def validate_batch_records(data: List[Dict[str, Any]], schema: Dict[str, type]) -> Dict[str, Any]: #funcao para validar um lote de registros 
     # Validações iniciais
    if not isinstance(data, list):
        logger.error("Dados devem ser uma lista")
        return {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'invalid_lines': [],
            'success_rate': 0.0,
            'is_valid': False,
            'validation_errors': ['Dados não são uma lista válida']
        }
    
    total_records = len(data)
    if total_records == 0:
        logger.warning("Lista de dados está vazia")
        return {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'invalid_lines': [],
            'success_rate': 0.0,
            'is_valid': True,  # Tecnicamente não há erros
            'validation_errors': []
        }
    
    # Inicialização das variáveis de controle
    valid_count = 0
    invalid_indices = []
    validation_errors = []
    
    logger.info(f"🔍 Iniciando validação em lote de {total_records} registros")
    
    # Processamento dos registros
    for index, record in enumerate(data):
        line_number = index + 1  # +1 para linha real (considerando possível header)
        
        try:
            if validate_schema(record, schema):
                valid_count += 1
                logger.debug(f"Registro linha {line_number}: VÁLIDO")
            else:
                invalid_indices.append(line_number)
                error_msg = f"Linha {line_number}: Schema inválido"
                if len(validation_errors) < 10:  # Limita erros para não sobrecarregar log
                    validation_errors.append(error_msg)
                logger.debug(f"Registro linha {line_number}: INVÁLIDO")
                
        except Exception as e:
            invalid_indices.append(line_number)
            error_msg = f"Linha {line_number}: Erro inesperado - {str(e)}"
            if len(validation_errors) < 10:
                validation_errors.append(error_msg)
            logger.error(f"Erro inesperado na linha {line_number}: {str(e)}")
    
    # Cálculos finais
    invalid_count = len(invalid_indices)
    success_rate = round((valid_count / total_records * 100), 2) if total_records > 0 else 0.0
    is_completely_valid = invalid_count == 0
    
    # Logging dos resultados
    if is_completely_valid:
        logger.info(f"✅ Validação concluída: Todos os {total_records} registros são válidos (100%)")
    else:
        logger.warning(f"⚠️ Validação concluída: {valid_count}/{total_records} registros válidos ({success_rate}%)")
        
        # Log das linhas inválidas (limitado para não poluir)
        if len(invalid_indices) <= 20:
            logger.warning(f"Registros inválidos nas linhas: {invalid_indices}")
        else:
            logger.warning(f"Registros inválidos: {invalid_count} linhas (primeiras 20: {invalid_indices[:20]}...)")
    
    # Se há muitos erros, adiciona resumo
    if len(validation_errors) == 10 and invalid_count > 10:
        validation_errors.append(f"... e mais {invalid_count - 10} erros similares")
    
    return {
        'total_records': total_records,
        'valid_records': valid_count,
        'invalid_records': invalid_count,
        'invalid_lines': invalid_indices,
        'success_rate': success_rate,
        'is_valid': is_completely_valid,
        'validation_errors': validation_errors
    }

def validate_required_fields(record: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Valida se todos os campos obrigatórios estão presentes e não vazios.
    
    Args:
        record: Dicionário representando um registro
        required_fields: Lista com nomes dos campos obrigatórios
        
    Returns:
        bool: True se todos os campos obrigatórios são válidos, False caso contrário
        
    Example:
        >>> required = ["name", "email"]
        >>> record = {"name": "João", "email": "joao@email.com", "age": 30}
        >>> validate_required_fields(record, required)
        True
    """
    if not isinstance(record, dict):
        logger.error(f"Registro deve ser um dicionário, recebido: {type(record)}")
        return False
    
    if not isinstance(required_fields, list):
        logger.error(f"Campos obrigatórios devem ser uma lista, recebido: {type(required_fields)}")
        return False
    
    if not required_fields:
        logger.debug("Nenhum campo obrigatório definido")
        return True
    
    logger.debug(f"🔍 Validando {len(required_fields)} campos obrigatórios: {required_fields}")
    
    missing_fields = []
    empty_fields = []
    
    for field in required_fields:
        if field not in record:
            missing_fields.append(field)
        elif record[field] is None:
            empty_fields.append(f"{field} (None)")
        elif isinstance(record[field], str) and record[field].strip() == '':
            empty_fields.append(f"{field} (vazio)")
        else:
            # Campo presente e não vazio
            logger.debug(f"✅ Campo obrigatório '{field}': OK")
    
    # Relatório de erros
    all_issues = []
    if missing_fields:
        issue = f"Campos ausentes: {missing_fields}"
        all_issues.append(issue)
        logger.error(f"❌ {issue}")
    
    if empty_fields:
        issue = f"Campos vazios: {empty_fields}"
        all_issues.append(issue)
        logger.error(f"❌ {issue}")
    
    if all_issues:
        logger.error(f"Validação de campos obrigatórios falhou: {'; '.join(all_issues)}")
        return False
    
    logger.debug(f"✅ Todos os {len(required_fields)} campos obrigatórios estão válidos")
    return True

