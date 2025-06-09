import os   #biblioteca para trabalhar com arquivos
import csv  #biblioteca para trabalhar com arquivos csv
import json #biblioteca para trabalhar com arquivos json
import logging #biblioteca para trabalhar com logs
from etl.utils import (
                        validate_csv_exists, 
                        validate_data, 
                        validate_schema, 
                        validate_required_fields,
                        validate_batch_records
                 ) #funcoes auxiliares para validar os arquivos, os dados e o schema de um dicionario 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

class ETL: # classe para realizar a extração, transformação  de CSV -> JSON 
    def __init__(self, input_path, output_path):  # construtor da classe
        self.input_path = input_path              # atributos da classe
        self.output_path = output_path            # atributos da classe

        # Schema esperado - movido para o construtor para melhor organização
        self.schema = {
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
            "LONG": float,
        }
                # Campos obrigatórios (você pode personalizar conforme sua necessidade)
        self.required_fields = [
            "F_NAME", 
            "L_NAME", 
            "EMAIL", 
            "PHONE"
        ]



    def extract(self):   # funcao para extrair os dados
        logging.info(f"📂 Extraindo dados de {self.input_path}") #mostra  de onde está extraindo os dados 
        validate_csv_exists(self.input_path) #valida se o arquivo CSV existe

        try:
            with open(self.input_path, mode='r', encoding='utf-8') as file: #abre o arquivo
                reader = csv.DictReader(file) #le o arquivo CSV e transforma em um dicionario
                data = list(reader) #transforma o arquivo em uma lista de dicionários

            logging.info(f"✅ {len(data)} registros extraídos.")  #mostra quantos registros foram encontrados
            return data
        
        except Exception as e:
            logging.error(f"❌ Erro ao extrair dados: {e}")
            raise 

    def transform(self, data):  # Função para transformar e validar os dados
        logger.info("🔧 Transformando dados...")

        # Validação inicial dos dados
        if not validate_data(data):
            raise ValueError("Nenhum dado extraído ou dados inválidos.")

        # NOVA IMPLEMENTAÇÃO: Validação prévia em lote
        logger.info("🔍 Executando validação prévia em lote...")
        
        # Primeiro, limpa os dados (remove espaços)
        cleaned_data = []
        for row in data:
            cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
            cleaned_data.append(cleaned_row)

        # Validação em lote dos dados limpos
        validation_report = validate_batch_records(cleaned_data, self.schema)
        
        # Log do relatório de validação
        logger.info(f"📊 Relatório de Validação Inicial:")
        logger.info(f"   • Total de registros: {validation_report['total_records']}")
        logger.info(f"   • Registros válidos: {validation_report['valid_records']}")
        logger.info(f"   • Registros inválidos: {validation_report['invalid_records']}")
        logger.info(f"   • Taxa de sucesso: {validation_report['success_rate']:.1f}%")
        
        if validation_report['invalid_lines']:
            logger.warning(f"   ⚠️ Linhas com problemas: {validation_report['invalid_lines']}")

        # Processa cada registro individualmente
        transformed_data = []
        valid_count = 0
        invalid_count = 0

        for i, row in enumerate(cleaned_data):
            try:
                # Validação de campos obrigatórios
                if not validate_required_fields(row, self.required_fields):
                    logger.warning(f"⚠️ Registro {i+1} ignorado: campos obrigatórios ausentes")
                    invalid_count += 1
                    continue

                # Validação de schema (verificação adicional individual)
                if validate_schema(row, self.schema):
                    transformed_data.append(row)
                    valid_count += 1
                else:
                    logger.warning(f"⚠️ Registro {i+1} ignorado: schema inválido")
                    invalid_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Erro ao processar registro {i+1}: {e}")
                invalid_count += 1
                continue

        # Relatório final da transformação
        total_processed = valid_count + invalid_count
        final_success_rate = (valid_count / total_processed * 100) if total_processed > 0 else 0
        
        logger.info(f"✅ Transformação concluída:")
        logger.info(f"   • Registros processados: {total_processed}")
        logger.info(f"   • Registros válidos finais: {valid_count}")
        logger.info(f"   • Registros rejeitados: {invalid_count}")
        logger.info(f"   • Taxa de sucesso final: {final_success_rate:.1f}%")

        if not transformed_data:
            raise ValueError("Nenhum registro válido após transformação")

        return transformed_data

    def load(self, data):   #funcao para carregar os dados transformados em um arquivo JSON
            logger.info(f"💾 Salvando dados em {self.output_path}")  # imprime onde estao salvando os dados
            
            try:
                os.makedirs(os.path.dirname(self.output_path), exist_ok=True) #cria o diretório onde o arquivo vai ser salvo

                with open(self.output_path, mode='w', encoding='utf-8') as file: #abre o arquivo
                    json.dump(data, file, indent=4, ensure_ascii=False) #salva o arquivo
                logging.info(f"✅ Dados salvos em {self.output_path}")

            except Exception as e:
                logging.error(f"❌ Erro ao salvar dados: {e}")
                raise
        

    def run(self):  #funcao para executar o ETL
        try:
            data = self.extract()  #extrai os dados
            validate_data (data)    #valida os dados
            data = self.trasform(data)   #limapa os dados
            self.load(data) #carrega os dados em um arquivo JSON

        except Exception as e:
            logging.error(f"❌ Erro ao executar o ETL: {e}")
            raise

        logging.info("🎉 ETL concluido com sucesso!")
    def get_validation_summary(self, data): #funcao para obter um resumo da validação
        logger.info("🔍 Obtendo resumo de validação...")
        if not data:
            return {"error": "Dados vazios"}
            
        # Limpa os dados primeiro
        cleaned_data = []
        for row in data:
            cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
            cleaned_data.append(cleaned_row)
        
        # Validação em lote
        batch_report = validate_batch_records(cleaned_data, self.schema)
        
        # Validação de campos obrigatórios
        missing_required_count = 0
        for row in cleaned_data:
            if not validate_required_fields(row, self.required_fields):
                missing_required_count += 1
        
        return {
            **batch_report,
            'missing_required_fields_count': missing_required_count,
            'required_fields': self.required_fields,
            'schema_fields': list(self.schema.keys())
        }


if __name__ == "__main__":  #verifica se o arquivo foi executado diretamente como um script principal   
    input_file_name = 'CRM_profiles.csv'
    input_file = os.path.join('data', 'input', input_file_name)
    output_file = os.path.join('data', 'output', input_file_name.replace('.csv', '.json'))


    etl = ETL(input_file, output_file)  #cria uma instancia da classe
    etl.run()   #executa o ETL


