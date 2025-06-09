import os   #biblioteca para trabalhar com arquivos
import csv  #biblioteca para trabalhar com arquivos csv
import json #biblioteca para trabalhar com arquivos json
import logging #biblioteca para trabalhar com logs
from etl.utils import validate_csv_exists, validate_data, validate_schema #funcoes auxiliares para validar os arquivos, os dados e o schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
class ETL: # classe para realizar a extração, transformação  de CSV -> JSON 
    def __init__(self, input_path, output_path):  # construtor da classe
        self.input_path = input_path              # atributos da classe
        self.output_path = output_path            # atributos da classe

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
        logging.info("🔧 Transformando dados...")

        if not data:
            logging.warning("❌ Nenhum dado extraído.")
            raise ValueError("Nenhum dado extraído.")

        transformed_data = []  # Lista para armazenar os dados transformados

        # Definição do schema esperado
        schema = {
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

        for row in data:
            # Remove espaços dos nomes e valores das chaves
            new_row_transformed = {k.strip(): v.strip() for k, v in row.items()}

            # Valida o schema
            if validate_schema(new_row_transformed, schema):
                transformed_data.append(new_row_transformed)
            else:
                logging.warning(f"⚠️ Registro ignorado por schema inválido: {new_row_transformed}")

        logging.info("✅ Transformação concluída.")
        return transformed_data #retorna os dados transformados e validados preise

    def load(self, data):   #funcao para carregar os dados transformados em um arquivo JSON
            logging.info(f"📂 Salvando dados em {self.output_path}")  # imprime onde estao salvando os dados
            
            try:
                os.makedirs(os.path.dirname(self.output_path), exist_ok=True) #cria o diretório onde o arquivo vai ser salvo

                with open(self.output_path, mode='w', encoding='utf-8') as file: #abre o arquivo
                    json.dump(data, file, indent=4, ensure_ascii=False) #salva o arquivo
                logging.info(f"✅ Dados salvos em {self.output_path}")
            except Exception as e:
                logging.error(f"❌ Erro ao salvar dados: {e}")
                raise
        

    def run(self):  #funcao para executar o ETL
            data = self.extract()  #extrai os dados
            validate_data (data)    #valida os dados
            data = self.trasform(data)   #limapa os dados
            self.load(data) #carrega os dados em um arquivo JSON


if __name__ == "__main__":  #verifica se o arquivo foi executado diretamente como um script principal   
    input_file_name = 'CRM_profiles.csv'
    input_file = os.path.join('data', 'input', input_file_name)
    output_file = os.path.join('data', 'output', input_file_name.replace('.csv', '.json'))


    etl = ETL(input_file, output_file)  #cria uma instancia da classe
    etl.run()   #executa o ETL


