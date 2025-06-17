# Integração Google Sheets com PostgreSQL

Este projeto fornece uma solução robusta para sincronizar dados entre planilhas do Google Sheets e um banco de dados PostgreSQL. É especialmente útil para automatizar a importação de dados de planilhas para um banco de dados relacional.

## Índice
- [Funcionalidades](#funcionalidades)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração do Google Cloud](#configuração-do-google-cloud)
- [Configuração do Banco de Dados](#configuração-do-banco-de-dados)
- [Configuração do Projeto](#configuração-do-projeto)
- [Uso](#uso)
- [Estrutura do Código](#estrutura-do-código)
- [Tratamento de Erros](#tratamento-de-erros)
- [Exemplos Práticos](#exemplos-práticos)
- [Contribuição](#contribuição)
- [Licença](#licença)

## Funcionalidades

- Leitura automática de planilhas do Google Sheets
- Suporte a múltiplos formatos de planilha (Google Sheets e XLSX)
- Processamento em lotes para melhor performance
- Tratamento de erros e reconexão automática
- Normalização automática de nomes de colunas
- Suporte a diferentes tipos de dados (monetários, datas, etc.)
- Logging detalhado das operações
- Sistema de retry para operações que podem falhar

## Pré-requisitos

- Python 3.8 ou superior
- Conta Google com acesso ao Google Sheets API
- Banco de dados PostgreSQL
- Credenciais do Google Cloud Platform (service-account.json)

## Instalação

1. Clone o repositório:
```bash
git clone [URL_DO_REPOSITÓRIO]
cd [NOME_DO_REPOSITÓRIO]
```

2. Crie e ative um ambiente virtual (recomendado):
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

## Configuração do Google Cloud

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Crie um novo projeto ou selecione um existente
3. Ative as APIs necessárias:
   - No menu lateral, vá em "APIs e Serviços" > "Biblioteca"
   - Na barra de pesquisa, procure por "Google Sheets API"
   - Clique no resultado e depois em "Ativar"
   - Volte para a biblioteca e procure por "Google Drive API"
   - Clique no resultado e depois em "Ativar"
   - Aguarde alguns minutos para que as APIs sejam ativadas completamente
4. Instale as bibliotecas do Google necessárias:
```bash
pip install google-api-python-client
pip install google-auth-httplib2
pip install google-auth-oauthlib
```
5. Crie uma conta de serviço:
   - Vá para "IAM & Admin" > "Service Accounts"
   - Clique em "Create Service Account"
   - Dê um nome e descrição
   - Conceda a role "Editor" (ou mais restritiva, se preferir)
6. Crie uma chave para a conta de serviço:
   - Clique na conta de serviço criada
   - Vá para a aba "Keys"
   - Clique em "Add Key" > "Create new key"
   - Escolha o formato JSON
   - Baixe o arquivo e renomeie para `service-account.json`
7. Coloque o arquivo `service-account.json` na raiz do projeto
8. Compartilhe suas planilhas do Google Drive com o email da conta de serviço:
   - Copie o email da conta de serviço (geralmente termina com @project-id.iam.gserviceaccount.com)
   - Abra suas planilhas no Google Drive
   - Clique em "Compartilhar"
   - Cole o email da conta de serviço
   - Dê permissão de "Editor"

## Configuração do Banco de Dados

1. Instale o PostgreSQL em sua máquina ou use um serviço de banco de dados
2. Crie um novo banco de dados:
```sql
CREATE DATABASE seu_banco;
```

3. Crie um usuário (opcional, mas recomendado):
```sql
CREATE USER seu_usuario WITH PASSWORD 'sua_senha';
GRANT ALL PRIVILEGES ON DATABASE seu_banco TO seu_usuario;
```

## Configuração do Projeto

1. Configure o arquivo `exemplo_sheets_to_postgres.py` com suas credenciais:

```python
# Configurações do banco de dados
DB_CONFIG = {
    'dbname': 'seu_banco',
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'host': 'seu_host',  # geralmente 'localhost' para instalação local
    'port': '5432'       # porta padrão do PostgreSQL
}

# ID da pasta do Google Drive onde estão as planilhas
# Você pode encontrar o ID na URL da pasta no Google Drive
# Exemplo: https://drive.google.com/drive/folders/abcdef
# O ID seria: abcdef
FOLDER_ID = 'seu_folder_id'

# Nome da tabela e coluna identificadora
TABLE_NAME = 'sua_tabela'
ID_COLUMN = 'codigo_da_transacao'
```

2. Ajuste a estrutura da tabela conforme suas necessidades:
```sql
CREATE TABLE sua_tabela (
    id SERIAL PRIMARY KEY,
    codigo_da_transacao VARCHAR(255) UNIQUE,
    data_criacao TIMESTAMP,
    valor DECIMAL(15, 2),
    status VARCHAR(100),
    descricao TEXT
);
```

## Uso

1. Prepare suas planilhas:
   - Certifique-se de que as planilhas estão na pasta do Google Drive especificada
   - A primeira linha deve conter os cabeçalhos das colunas
   - Os nomes das colunas serão normalizados automaticamente

2. Configure as IDs das planilhas que deseja processar:
```python
sheet_ids = [
    'seu_sheet_id_1',  # ID da primeira planilha
    'seu_sheet_id_2'   # ID da segunda planilha
]
```

3. Execute o script:
```bash
python exemplo_sheets_to_postgres.py
```

## Estrutura do Código

- `verify_folder_access()`: Verifica acesso à pasta do Google Drive
- `get_sheets_from_folder()`: Lista todas as planilhas na pasta
- `get_sheet_data()`: Lê os dados de uma planilha específica
- `normalize_column_name()`: Normaliza nomes de colunas
- `clean_data()`: Limpa e formata os dados
- `insert_to_postgres()`: Insere dados no PostgreSQL
- `setup_database()`: Configura a estrutura do banco de dados

## Tratamento de Erros

O script inclui tratamento robusto de erros:
- Reconexão automática com o banco de dados
- Retry em caso de falhas na leitura das planilhas
- Logging detalhado de erros
- Rollback de transações em caso de falha

## Exemplos Práticos

### Exemplo 1: Planilha de Vendas
```python
# Estrutura da tabela para vendas
CREATE TABLE vendas (
    id SERIAL PRIMARY KEY,
    codigo_venda VARCHAR(255) UNIQUE,
    data_venda TIMESTAMP,
    valor_total DECIMAL(15, 2),
    status_pagamento VARCHAR(100),
    cliente VARCHAR(255)
);

# Configuração das colunas monetárias
monetary_cols = [
    'valor_total',
    'valor_frete',
    'valor_desconto'
]

# Configuração das colunas de data
date_cols = [
    'data_venda',
    'data_pagamento',
    'data_envio'
]
```

### Exemplo 2: Planilha de Produtos
```python
# Estrutura da tabela para produtos
CREATE TABLE produtos (
    id SERIAL PRIMARY KEY,
    codigo_produto VARCHAR(255) UNIQUE,
    nome VARCHAR(255),
    preco DECIMAL(15, 2),
    estoque INTEGER,
    categoria VARCHAR(100)
);

# Configuração das colunas monetárias
monetary_cols = [
    'preco',
    'preco_custo',
    'preco_promocional'
]

# Configuração das colunas de data
date_cols = [
    'data_cadastro',
    'data_atualizacao'
]
```

## Contribuição

Embora eu não desejo modificações no github, sinta-se a vontade de modificar ele para o seu uso pessoal

## Licença

Esse projeto foi realizado para transferir dados de uma empresa que eu trabalho, mas sinta-se livre de usar
