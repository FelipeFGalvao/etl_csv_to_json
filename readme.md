# ETL de CSV para JSON

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?style=for-the-badge&logo=python)
![Tests](https://img.shields.io/badge/Tests-Pytest-green?style=for-the-badge&logo=pytest)
![Coverage](https://img.shields.io/badge/Coverage-95%25-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-purple?style=for-the-badge)

> Pipeline de ETL (Extract, Transform, Load) construído em Python. Este projeto processa arquivos CSV de perfis de clientes, aplica validações rigorosas de schema e transforma os dados, salvando o resultado em formato JSON. Idealizado como uma peça de portfólio para demonstrar boas práticas de desenvolvimento, incluindo testes automatizados, logging detalhado e código modular.

<br>

##  navegar
* [📌 Sobre o Projeto](#-sobre-o-projeto)
* [✨ Funcionalidades](#-funcionalidades)
* [⚙️ O Processo de Transformação em Detalhes](#️-o-processo-de-transformação-em-detalhes)
* [💻 Tecnologias Utilizadas](#-tecnologias-utilizadas)
* [🚀 Como Executar o Projeto](#-como-executar-o-projeto)
* [🧪 Como Rodar os Testes](#-como-rodar-os-testes)
* [📁 Estrutura de Pastas](#-estrutura-de-pastas)
* [🧠 Próximos Passos](#-próximos-passos)
* [👨‍💻 Autor](#-autor)

---

## 📌 Sobre o Projeto

O objetivo deste ETL é garantir a integridade e a qualidade dos dados de perfis de clientes antes de serem consumidos por outros sistemas. O pipeline extrai dados de um arquivo `CRM_profiles.csv`, executa um processo de validação e limpeza em duas etapas, e carrega os dados 100% conformes em um arquivo `CRM_profiles.json`. Erros e avisos são registrados em logs para total rastreabilidade do processo.

---

## ✨ Funcionalidades

-   **Extração (Extract):** Leitura eficiente de dados de arquivos `.csv` utilizando `DictReader` para processar registros como dicionários. Valida a existência e o formato do arquivo de entrada.
-   **Transformação (Transform):**
    -   **Limpeza de Dados:** Remove automaticamente espaços em branco desnecessários das chaves e valores de cada registro.
    -   **Validação em Lote:** Realiza uma validação prévia em todos os dados para gerar um relatório rápido sobre a saúde geral do arquivo, com a taxa de sucesso inicial.
    -   **Validação Individual com Lógica Avançada:** Cada registro é verificado para garantir a presença de campos obrigatórios (diferenciando campos ausentes de campos vazios) e a conformidade com o schema, incluindo conversões de tipo inteligentes.
    -   **Tolerância a Falhas:** Registros inválidos são descartados e logados como `warning` sem interromper o pipeline, garantindo que todos os dados válidos sejam processados.
-   **Carga (Load):**
    -   Criação automática do diretório de saída, se não existir.
    -   Salvamento dos dados limpos e válidos em formato `.json` legível e bem formatado.
-   **Logging Detalhado e Estruturado:** Registra cada etapa (`INFO`), avisos de registros inválidos (`WARNING`) e erros críticos (`ERROR`), com timestamps para fácil depuração. Ao final, exibe um relatório de validação consolidado.

---

## ⚙️ O Processo de Transformação em Detalhes

A etapa de transformação é o coração deste projeto e segue um fluxo robusto para garantir a máxima qualidade dos dados:

1.  **Limpeza Inicial:** Antes de qualquer validação, todos os registros passam por uma limpeza, onde espaços em branco no início e no fim das chaves e valores de texto são removidos.

2.  **Validação em Lote (Prévia):** O sistema realiza uma primeira análise em todos os registros para gerar um relatório de saúde dos dados. Isso oferece uma visão macro da qualidade do arquivo de entrada, com a porcentagem de registros conformes.

3.  **Validação Individual:** Cada registro é então validado individualmente contra dois critérios principais:
    -   **Presença de Campos Obrigatórios:** Verifica se os seguintes campos existem: `F_NAME`, `L_NAME`, `EMAIL`, `PHONE`. O sistema diferencia campos que **não existem** no registro de campos que existem mas estão **vazios** (`None` ou `''`), gerando logs específicos para cada caso.
    -   **Conformidade com o Schema:** Garante que cada campo corresponde ao tipo de dado esperado. A validação de tipo é robusta, capaz de, por exemplo, converter um valor como `"50.0"` para o inteiro `50`, e permite que campos não obrigatórios sejam nulos ou vazios.

4.  **Tratamento e Relatório Final:** Registros que falham em qualquer uma das validações individuais são descartados, e uma entrada de log (`WARNING`) é gerada. Ao final do processo, um relatório consolidado e formatado é exibido no console, apresentando um resumo claro do resultado da operação.

<details>
<summary>📖 Clique para ver o Schema de Validação Completo</summary>

```python
{
    "TITLE": str, "F_NAME": str, "L_NAME": str, "GENDER": str,
    "MONTH_AND_DATE": str, "DOB": str, "YOB": int, "EMAIL": str,
    "ID1": str, "ID2": str, "ID3": str, "ID4": str, "PHONE": str,
    "EMAIL2": str, "STREET": str, "CITY": str, "STATE": str,
    "COUNTRY": str, "ZIP": str, "LAT": float, "LONG": float,
}
```
</details>

---

## 💻 Tecnologias Utilizadas

Este projeto foi construído com as seguintes tecnologias:

-   **Python 3.11+**
-   **Pytest** (para testes unitários)
-   **pytest-cov** (para relatório de cobertura de testes)
-   Módulos Nativos: `typing`, `logging`, `csv`, `json`, `pathlib`

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
│   └── utils.py        # Funções auxiliares de validação
│
├── tests/              # Suíte de testes automatizados
│   └── test_etl.py     # Testes unitários para o pipeline
│
├── .gitignore          # Arquivos e pastas a serem ignorados pelo Git
└── readme.md           # Documentação do projeto
```

---

## 🧠 Próximos Passos

-   [ ] Integração com um banco de dados (ex: PostgreSQL, SQLite) como destino (Load).
-   [ ] Upload automático do arquivo JSON gerado para um serviço de nuvem (ex: AWS S3).
-   [ ] Criação de uma Interface de Linha de Comando (CLI) com `argparse` para passar argumentos (ex: caminho do arquivo de entrada/saída).
-   [ ] Orquestração do pipeline com ferramentas como Airflow ou Prefect.

---

## 👨‍💻 Autor

**Felipe Galvão**

-   LinkedIn: [`linkedin.com/in/felipe-galvao-data`](https://www.linkedin.com/in/felipe-galvao-data/)
-   GitHub: [`github.com/felipegalvao`](https://github.com/felipegalvao)