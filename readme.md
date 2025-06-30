# ETL de CSV para JSON

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?style=for-the-badge&logo=python)
![Tests](https://img.shields.io/badge/Tests-Pytest-green?style=for-the-badge&logo=pytest)
![Coverage](https://img.shields.io/badge/Coverage-85%25-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-purple?style=for-the-badge)

> Pipeline de ETL (Extract, Transform, Load) construído em Python. Este projeto processa arquivos CSV de perfis de clientes, aplica validações rigorosas de schema e transforma os dados, salvando o resultado em formato JSON. Idealizado como uma peça de portfólio para demonstrar boas práticas de desenvolvimento, incluindo testes automatizados e código modular.

<br>

##  navegar
* [📌 Sobre o Projeto](#-sobre-o-projeto)
* [✨ Funcionalidades](#-funcionalidades)
* [💻 Tecnologias Utilizadas](#-tecnologias-utilizadas)
* [🚀 Como Executar o Projeto](#-como-executar-o-projeto)
* [🧪 Como Rodar os Testes](#-como-rodar-os-testes)
* [📁 Estrutura de Pastas](#-estrutura-de-pastas)
* [🧠 Próximos Passos](#-próximos-passos)
* [👨‍💻 Autor](#-autor)

---

## 📌 Sobre o Projeto

O objetivo deste ETL é garantir a integridade e a qualidade dos dados que entram em um sistema. O pipeline extrai dados de um arquivo `CRM_profiles.csv`, valida se cada registro atende a um schema pré-definido (campos obrigatórios, tipos de dados), realiza transformações como limpeza de espaços e padronização, e carrega os dados validados em um arquivo `CRM_profiles.json`. Erros de validação são registrados em logs para fácil rastreabilidade.

---

## ✨ Funcionalidades

-   **Extração (Extract):** Leitura de dados a partir de arquivos `.csv`.
-   **Validação de Schema:** Garante que os dados de entrada possuam a estrutura correta (campos, tipos, etc.).
-   **Transformação (Transform):** Limpeza de dados, como remoção de espaços em branco e tratamento de valores nulos.
-   **Carga (Load):** Salvamento dos dados processados e válidos em formato `.json`.
-   **Logging Detalhado:** Registra cada etapa do processo, incluindo erros de validação e sucesso na execução.
-   **Testes Unitários:** Cobertura de testes robusta para as funções de extração, transformação e carga, garantindo a confiabilidade do pipeline.

---

## 💻 Tecnologias Utilizadas

Este projeto foi construído com as seguintes tecnologias:

-   **Python 3.11+**
-   **Pytest** (para testes unitários)
-   **pytest-cov** (para relatório de cobertura de testes)
-   Módulo `typing` para tipagem estática
-   Módulo `logging` para logs estruturados
-   Módulo `csv` e `json` para manipulação de arquivos

---

## 🚀 Como Executar o Projeto

Siga os passos abaixo para executar o projeto localmente.

### **Pré-requisitos**

-   [Python 3.11](https://www.python.org/downloads/) ou superior
-   [Git](https://git-scm.com/downloads)

### **Passo a Passo**

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/ETL_CSV_TO_JSON.git](https://github.com/seu-usuario/ETL_CSV_TO_JSON.git)
    cd ETL_CSV_TO_JSON
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    # Linux / macOS
    python3 -m venv venv
    source venv/bin/activate

    # Windows
    python -m venv venv
    .\venv\Scripts\activate
    ```

3.  **Instale as dependências:**
    *(Observação: Crie um arquivo `requirements.txt` com as bibliotecas `pytest` e `pytest-cov`)*
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute o Pipeline ETL:**
    O script principal irá ler o arquivo de `data/input/CRM_profiles.csv` e gerar a saída em `data/output/CRM_profiles.json`.
    ```bash
    python etl/main.py
    ```

---

## 🧪 Como Rodar os Testes

Os testes unitários foram criados para validar cada componente do pipeline.

Para executar os testes e ver o relatório de cobertura no terminal, rode o comando na raiz do projeto:

```bash
pytest --cov=etl --cov-report term-missing
```

---

## 📁 Estrutura de Pastas

O projeto está organizado da seguinte forma para garantir clareza e manutenibilidade:

```
ETL_CSV_TO_JSON/
│
├── .pytest_cache/      # Cache gerado pelo Pytest
├── data/
│   ├── input/
│   │   └── CRM_profiles.csv  # Arquivo de dados brutos de entrada
│   └── output/
│       └── CRM_profiles.json # Arquivo de saída com dados processados
│
├── etl/                # Módulo principal da aplicação
│   ├── __init__.py
│   ├── main.py         # Ponto de entrada (entrypoint) para executar o pipeline
│   ├── pipeline.py     # Contém a classe e a lógica do ETL
│   └── utils.py        # Funções auxiliares (ex: validações)
│
├── tests/              # Suíte de testes automatizados
│   ├── __init__.py
│   └── test_etl.py     # Testes unitários para o pipeline
│
├── .gitignore          # Arquivos e pastas a serem ignorados pelo Git
└── readme.md           # Documentação do projeto
```

---

## 🧠 Próximos Passos

-   [ ] Integração com um banco de dados (ex: PostgreSQL, MongoDB) como destino (Load).
-   [ ] Upload automático do arquivo JSON gerado para um serviço de nuvem (ex: AWS S3).
-   [ ] Criação de uma Interface de Linha de Comando (CLI) para passar argumentos (ex: caminho do arquivo de entrada/saída).
-   [ ] Orquestração do pipeline com ferramentas como Airflow ou Prefect.

---

## 👨‍💻 Autor

**Felipe Galvão**

-   LinkedIn: [`linkedin.com/in/felipe-galvao`](https://linkedin.com/in/felipe-galvão)
-   GitHub: [`github.com/felipegalvao`](https://github.com/felipegalvao)