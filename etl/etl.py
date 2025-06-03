# src/etl.py
import os   #biblioteca para trabalhar com arquivos
import csv  #biblioteca para trabalhar com arquivos csv
import json #biblioteca para trabalhar com arquivos json


class ETL: # classe para realizar a extração, transformação  de CSV -> JSON 
    def __init__(self, input_path, output_path):  # construtor da classe
        self.input_path = input_path              # atributos da classe
        self.output_path = output_path            # atributos da classe
    def extract(self):   # funcao para extrair os dados
        print(f"📥 Extraindo dados de {self.input_path}") #mostra  de onde está extraindo os dados 
        with open(self.input_path, mode='r', encoding='utf-8') as file: #abre o arquivo
            reader = csv.DictReader(file) #le o arquivo CSV e transforma em um dicionario
            data = list(reader) #transforma o arquivo em uma lista de dicionários
        print(f"✅ Dados extraídos: {len(data)} registros encontrados.")  #mostra quantos registros foram encontrados
        return data 

    def clean(self, data): #funcao para transformar os dados
        print("🔧 Transformando dados...")
        transformed_data = []   #cria uma lista que será usada para armazenar os dados transformados
        for row in data:        #percorre a lista
            new_row_clean = {k.strip(): v.strip() for k, v in row.items()} # remove os espaços em branco no início e fim de cada chave(k) e valor(v) de cada registro
            transformed_data.append(new_row_clean) #adiciona cada linha já limpa, a lista de dados transformados
        print("✅ Transformação concluída.")
        return transformed_data

    def load(self, data):   #funcao para carregar os dados transformados em um arquivo JSON
        print(f"💾 Salvando dados em {self.output_path}")   # imprime onde estao salvando os dados
        with open(self.output_path, mode='w', encoding='utf-8') as file: #abre o arquivo
            json.dump(data, file, indent=4, ensure_ascii=False) #salva o arquivo
        print("✅ Dados salvos com sucesso.")

    def run(self):  #funcao para executar o ETL
        data = self.extract()  #extrai os dados
        data = self.clean(data)   #limapa os dados
        self.load(data) #carrega os dados em um arquivo JSON


if __name__ == "__main__":  #verifica se o arquivo foi executado diretamente como um script principal   
    input_file_name = 'CRM_profiles.csv'
    input_file = os.path.join('data', 'input', input_file_name)
    output_file = os.path.join('data', 'output', input_file_name.replace('.csv', '.json'))


    etl = ETL(input_file, output_file)  #cria uma instancia da classe
    etl.run()   #executa o ETL


