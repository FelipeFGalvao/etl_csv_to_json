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
    # Inicializa variáveis
    total_records = len(data)
    valid_count = 0
    invalid_indices = []
    
    logger.info(f"Iniciando validação em lote de {total_records} registros")
    
    for index, record in enumerate(data):
        if validate_schema(record, schema):
            valid_count += 1
        else:
            invalid_indices.append(index + 1)  # +1 para linha real (considerando header)
    
    invalid_count = len(invalid_indices)
    success_rate = (valid_count / total_records * 100) if total_records > 0 else 0
    
    # Log do resultado
    if invalid_count == 0:
        logger.info(f"✅ Validação concluída: Todos os {total_records} registros são válidos")
    else:
        logger.warning(f"⚠️ Validação concluída: {valid_count}/{total_records} registros válidos ({success_rate:.1f}%)")
        logger.warning(f"Registros inválidos nas linhas: {invalid_indices}")
    
    return {
        'total_records': total_records,
        'valid_records': valid_count,
        'invalid_records': invalid_count,
        'invalid_lines': invalid_indices,
        'success_rate': success_rate,
        'is_valid': invalid_count == 0
    }
def validate_required_fields(record: Dict[str, Any], required_fields: List[str]) -> bool: #funcao para validar os campos obrigatorios de um dicionario  
    logger.debug(f"Validando campos obrigatórios: {required_fields}")
    missing_fields = []
    
    for field in required_fields:
        if field not in record or record[field] is None or str(record[field]).strip() == '':
            missing_fields.append(field)
    
    if missing_fields:
        logger.error(f"Campos obrigatórios ausentes ou vazios: {missing_fields}")
        return False
    
    logger.debug(f"Todos os campos obrigatórios estão presentes: {required_fields}")
    return True
