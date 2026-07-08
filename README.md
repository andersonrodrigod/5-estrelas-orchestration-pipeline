# 5 Estrelas

Pipeline em Python para processar a base do projeto 5 Estrelas, desde a extração Oracle até a geração das bases de pré-validação, auditoria, arquivos para Power BI e separação por grupos de classificação.

O projeto foi organizado para permitir dois modos de trabalho:

- execução individual, etapa por etapa, útil para depuração e auditoria;
- execução em pipeline, que reaproveita as mesmas funções das etapas individuais e grava os artefatos finais consolidados.

## Visão Geral

Fluxo principal:

1. Extração Oracle opcional em `scripts/extracao_oracle_exemplo.py`.
2. Entrada das bases em `data/manual` ou `data/oracle`.
3. Pré-validação de avaliações.
4. Pré-validação de negativas.
5. Geração do CSV e dos Excels para Power BI.
6. Separação de avaliações e negativas por grupos de classificação.
7. Geração de auditorias e resumos em `auditoria`.

## Estrutura

```text
.
+-- execucoes_pipeline/
|   +-- config.py
|   +-- exec_pre_validacao.py
|   +-- exec_pre_validacao_avaliacoes.py
|   +-- exec_pre_validacao_negativas.py
|   +-- exec_gerar_arquivos_bi.py
|   +-- exec_separacao.py
+-- execucoes_individuais_avaliacoes/
+-- execucoes_individuais_negativas/
+-- funcoes_auxiliares/
+-- scripts/
+-- doc/
+-- data/
+-- data_exec/
+-- auditoria/
```

Pastas de dados e execução:

- `data`: entradas locais, extrações e arquivos auxiliares. Não deve ser versionada.
- `data_exec`: saídas geradas pela execução. Não deve ser versionada.
- `auditoria`: resumos, inspeções e evidências de validação. Não deve ser versionada.
- `utils/insumos`: arquivos de regra e de-para quando usados localmente.

## Dependências

Projeto desenvolvido em Python.

Dependências principais:

```bash
pip install pandas openpyxl oracledb
```

`oracledb` é necessário apenas para executar a extração Oracle.

## Configuração

Os caminhos principais ficam centralizados em:

```text
execucoes_pipeline/config.py
```

A função `criar_config_pre_validacao()` monta um objeto de configuração com entradas, saídas, arquivos de insumo e pastas de auditoria.

Entradas padrão:

- Avaliações: `data/manual/junho_incompleto_5_estrelas.csv`
- Negativas: `data/manual/5_estrelas_maio_negativo.csv`

Também é possível alterar as entradas por variável de ambiente:

```powershell
$env:PIPELINE_AVALIACOES_ENTRADA = "data/manual/minha_base_avaliacoes.csv"
$env:PIPELINE_NEGATIVAS_ENTRADA = "data/manual/minha_base_negativas.csv"
```

## Extração Oracle

Script:

```bash
python scripts/extracao_oracle_exemplo.py
```

Configuração local:

```text
scripts/.env.oracle
```

Esse arquivo não deve ser versionado, pois pode conter usuário, senha, DSN e outros dados sensíveis.

Saídas esperadas:

- CSV extraído em `data/oracle`.
- Log da extração em `data/oracle/log_extracao_oracle.xlsx`.

## Pré-Validação

### Avaliações

```bash
python execucoes_pipeline/exec_pre_validacao_avaliacoes.py
```

Saída principal:

```text
data_exec/pipeline/pipeline_pre_validacao_avaliacoes_base_final.csv
```

Auditorias:

```text
auditoria/saida_resumo_pipeline/exec_pre_validacao_avaliacoes
```

### Negativas

```bash
python execucoes_pipeline/exec_pre_validacao_negativas.py
```

Saída principal:

```text
data_exec/pipeline/pipeline_pre_validacao_negativas_base_final.csv
```

Auditorias:

```text
auditoria/saida_resumo_pipeline/exec_pre_validacao_negativas
```

## Geração dos Arquivos de BI

```bash
python execucoes_pipeline/exec_gerar_arquivos_bi.py
```

Entrada:

```text
data_exec/pipeline/pipeline_pre_validacao_avaliacoes_base_final.csv
```

Saídas:

```text
data_exec/pipeline/12_base_power_bi.csv
data_exec/excel_bi/13_base_tipo_1_a_3_power_bi.xlsx
data_exec/excel_bi/13_base_tipo_4_a_7_power_bi.xlsx
data_exec/excel_bi/13_base_tipo_8_ou_mais_power_bi.xlsx
```

Auditorias:

```text
auditoria/saida_resumo_pipeline/exec_gerar_arquivos_bi
```

## Separação por Grupo

```bash
python execucoes_pipeline/exec_separacao.py
```

Entradas:

```text
data_exec/pipeline/pipeline_pre_validacao_avaliacoes_base_final.csv
data_exec/pipeline/pipeline_pre_validacao_negativas_base_final.csv
data/nomes_classificacao.json
```

Saída local, quando a pasta SharePoint/OneDrive configurada não está disponível:

```text
data_exec/separacao
```

Auditorias:

```text
auditoria/saida_resumo_pipeline/exec_separacao
```

## Etapas da Pré-Validação de Avaliações

| Etapa | Script | Função principal |
| --- | --- | --- |
| 00 | `exec_00_verificacao_campos.py` | Verifica campos vazios, inválidos e inconsistências iniciais. |
| 01 | `exec_01_limpeza.py` | Padroniza colunas e remove linhas sem nota válida. |
| 02 | `exec_02_contratacao.py` | Preenche contratação e completa UF pela aba `contratacao`. |
| 03 | `exec_03_nota.py` | Calcula `NOTA GERAL`. |
| 04 | `exec_04_classificacao.py` | Aplica regras de classificação. |
| 05 | `exec_05_local_editado.py` | Preenche `LOCAL EDITADO`. |
| 06 | `exec_06_ajustes_finais.py` | Aplica ajustes finais após a classificação. |
| 07 | `exec_07_operadora.py` | Preenche ou revisa `OPERADORA`. |
| 08 | `exec_08_meta.py` | Preenche `META`. |
| 09 | `exec_09_resultado_unidade.py` | Calcula `RESULTADO DA UNIDADE`. |
| 10 | `exec_10_status_unidade.py` | Calcula `STATUS UNIDADE`. |
| 11 | `exec_11_analise_dados.py` | Gera análises consolidadas. |
| 12 | `exec_12_power_bi.py` | Gera o CSV no layout do Power BI. |
| 13 | `exec_13_separar_tipo_excel.py` | Gera os Excels separados por tipo. |

## Auditoria

As auditorias ajudam a explicar por que um dado foi preenchido, ficou vazio ou mudou durante o processo.

Pontos comuns de investigação:

- contratação ou UF vazia: consultar auditorias da etapa 02;
- classificação vazia ou incorreta: consultar auditorias da etapa 04;
- local editado vazio: consultar auditorias da etapa 05;
- operadora vazia ou alterada: consultar auditorias da etapa 07;
- meta, resultado ou status incoerente: consultar etapas 08, 09 e 10;
- divergência nos arquivos finais de BI: consultar etapas 12 e 13;
- classificação fora dos arquivos separados: consultar `exec_separacao_classificacoes_nao_enviadas.csv`.

## Documentação Detalhada

O fluxo completo, com explicação das etapas, arquivos gerados e critérios de auditoria, está em:

```text
doc/processo_5_estrelas.txt
```

## Observações de Versionamento

Não versionar dados de execução, bases reais ou auditorias geradas:

- `data/`
- `data_exec/`
- `auditoria/`
- `.env`
- `.env.*`

Esses caminhos podem conter dados sensíveis, arquivos grandes ou saídas variáveis de execução.
