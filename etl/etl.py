# src/etl.py

import os
import csv
import json


class ETL:              # classe para realizar a extração, transformação e carregamento de dados
    def __init__(self, input_path, output_path):  # construtor da classe
        self.input_path = input_path              # atributos da classe
        self.output_path = output_path            # atributos da classe
    def extract(self):   # funcao para extrair os dados
        print(f"📥 Extraindo dados de {self.input_path}") #mostra  de onde está extraindo os dados 
        with open(self.input_path, mode='r', encoding='utf-8') as file: #abre o arquivo
            reader = csv.DictReader(file) #le o arquivo
            data = list(reader) #transforma o arquivo em uma lista
        print(f"✅ Dados extraídos: {len(data)} registros encontrados.")  #mostra quantos registros foram encontrados
        return data 

    def transform(self, data): #funcao para transformar os dados
        print("🔧 Transformando dados...")
        transformed_data = []   #cria uma lista
        for row in data:        #percorre a lista
            new_row = {k.strip(): v.strip() for k, v in row.items()} #transforma a lista em um dicionario
            transformed_data.append(new_row) #adiciona o dicionario na lista
        print("✅ Transformação concluída.")
        return transformed_data

    def load(self, data):   #funcao para carregar os dados
        print(f"💾 Salvando dados em {self.output_path}")   # imprime onde estao salvando os dados
        with open(self.output_path, mode='w', encoding='utf-8') as file: #abre o arquivo
            json.dump(data, file, indent=4, ensure_ascii=False) #salva o arquivo
        print("✅ Dados salvos com sucesso.")

    def run(self):  #funcao para executar o ETL
        data = self.extract()  #extrai os dados
        data = self.transform(data)   #transforma os dados
        self.load(data) #carrega os dados


if __name__ == "__main__":  
    input_file_name = 'CRM_profiles.csv'
    input_file = os.path.join('data', 'input', input_file_name)
    output_file = os.path.join('data', 'output', input_file_name.replace('.csv', '.json'))


    etl = ETL(input_file, output_file)  #cria uma instancia da classe
    etl.run()   #executa o ETL
