import os
from etl.pipeline import ETL


if __name__ == "__main__":  #verifica se o arquivo foi executado diretamente como um script principal   
    input_file_name = 'CRM_profiles.csv'
    input_file = os.path.join('data', 'input', input_file_name)
    output_file = os.path.join('data', 'output', input_file_name.replace('.csv', '.json'))


    etl = ETL(input_file, output_file)  #cria uma instancia da classe
    etl.run()   #executa o ETL