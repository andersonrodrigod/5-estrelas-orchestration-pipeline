# Plano da Pipeline Otimizada de Avaliacoes

## Objetivo

Reduzir o tempo de execucao do fluxo de avaliacoes evitando a gravacao e releitura de bases gigantes entre cada etapa.

A pipeline nova deve manter os dados em memoria durante o processamento e gravar apenas:

- resumos e auditorias de todas as etapas;
- CSV final completo para analise;
- Excel de analise de dados;
- arquivos finais de Power BI somente na execucao final.

As execucoes individuais continuam existindo como modo de debug, auditoria pontual e compatibilidade.

## Branch de Trabalho

```text
refactor/pipeline-avaliacoes
```

## Pasta Nova

```text
execucoes_pipeline/
```

Estrutura prevista:

```text
execucoes_pipeline/
    __init__.py
    config.py
    pipeline.py
    resumos.py
    exec_pre_validacao.py
    exec_pre_validacao_avaliacoes.py
    exec_pre_validacao_negativas.py
    exec_gerar_arquivos_bi.py
    exec_separacao.py
```

## Principio de Arquitetura

As regras de negocio das execucoes atuais devem ser reaproveitadas como funcoes nos proprios modulos das execucoes individuais.

O modelo desejado para cada etapa e:

```text
entrada em memoria -> processamento -> saida em memoria + resumos
```

Os scripts individuais continuam sendo executaveis, mas tambem devem expor funcoes como:

```text
processar_etapa(...)
salvar_resumos_etapa(...)
executar(...)
```

A pipeline nova importa essas funcoes e apenas orquestra o dataframe em memoria.

Isso evita duplicar regra de negocio em dois lugares diferentes.

## Pre-validacao

A pre-validacao fica separada em dois entrypoints:

```text
execucoes_pipeline/exec_pre_validacao_avaliacoes.py
execucoes_pipeline/exec_pre_validacao_negativas.py
```

O arquivo abaixo permanece como agregador para rodar os dois fluxos em sequencia:

```text
execucoes_pipeline/exec_pre_validacao.py
```

A pre-validacao de avaliacoes comeca da entrada bruta usada pela execucao 00.

Ela deve executar o fluxo completo de avaliacoes ate a analise:

```text
00 verificacao campos
01 limpeza
02 contratacao
03 nota
04 classificacao
05 local editado
06 ajustes finais
07 operadora
08 meta
09 resultado unidade
10 status unidade
11 analise dados
```

Na mesma execucao, ela tambem deve executar o fluxo de negativas:

```text
00 verificacao campos negativas
01 limpeza negativas
02 contratacao negativas
03 classificacao negativas
04 local editado negativas
```

Ela deve gerar:

- todos os resumos e auditorias de todas as etapas;
- CSV final completo da base tratada;
- CSV final completo da base tratada de negativas;
- Excel de analise de dados;
- documentacao da analise de dados;
- resumo mestre da execucao com tempo por etapa e arquivos gerados.

Ela nao deve gerar:

- CSVs intermediarios gigantes de cada etapa;
- CSV Power BI;
- Excels separados por tipo para Power BI.

## Execucao Final

A execucao final roda depois dos ajustes manuais nos insumos e regras.

Ela deve gerar:

- todos os resumos e auditorias;
- CSV final completo da base tratada;
- Excel de analise de dados;
- CSV Power BI;
- tres Excels finais separados por tipo para Power BI;
- Excels separados por grupo/classificacao usando avaliacoes e negativas finais;
- resumo mestre da execucao com tempo por etapa e arquivos gerados.

Ela tambem nao deve gravar CSVs intermediarios gigantes, salvo se uma flag de debug for ativada.

## Saidas Propostas

Pre-validacao:

```text
data_exec_indiv/avaliacoes/pipeline_pre_validacao_base_final.csv
saida_resumo_avaliacoes/pipeline_pre_validacao/
data_exec_indiv/negativas/pipeline_pre_validacao_base_final.csv
saida_resumo_negativas/pipeline_pre_validacao/
```

Final:

```text
data_exec_indiv/avaliacoes/pipeline_final_base_final.csv
data_exec_indiv/avaliacoes/12_base_power_bi.csv
data_exec_indiv/avaliacoes/13_base_tipo_1_a_3_power_bi.xlsx
data_exec_indiv/avaliacoes/13_base_tipo_4_a_7_power_bi.xlsx
data_exec_indiv/avaliacoes/13_base_tipo_8_ou_mais_power_bi.xlsx
data_exec_indiv/separacao/
saida_resumo_avaliacoes/pipeline_final/
```

A separacao por grupo/classificacao dentro de `execucoes_pipeline/exec_separacao.py`
usa como entrada:

```text
data_exec_indiv/avaliacoes/12_base_power_bi.csv
data_exec_indiv/negativas/pipeline_pre_validacao_base_final.csv
```

## Resumos

Todos os resumos devem ser gerados mesmo na pre-validacao.

Essa decisao e intencional: os resumos sao pequenos, baratos de gerar e sao a principal ferramenta para ajustar manualmente insumos e regras ao longo do dia.

## Flags Previstas

```text
salvar_resumos = True
salvar_csv_final = True
salvar_bases_intermediarias = False
salvar_power_bi = False na pre-validacao, True na final
salvar_excel_tipo_power_bi = False na pre-validacao, True na final
```

## Regra de OneDrive Comercial

Arquivos de insumo devem manter a prioridade do OneDrive comercial via `OneDriveCommercial`.

Quando o arquivo existir em:

```text
OneDriveCommercial/5 Estrelas/INSUMOS
```

ele deve ser usado. Caso contrario, a pipeline deve usar o caminho local do projeto como fallback.

## Cuidados de Validacao

O refactor deve ser incremental.

Para cada bloco migrado:

- comparar contagens de linhas com a execucao individual correspondente;
- comparar principais totais dos resumos;
- manter nomes de colunas finais esperados;
- evitar mudanca de regra de negocio durante a extracao.

## Execucoes Individuais

As execucoes individuais permanecem como suporte para:

- depurar uma etapa especifica;
- gerar bases intermediarias sob demanda;
- comparar comportamento antigo e novo;
- reproduzir problemas pontuais.

Na nova branch, elas podem ser adaptadas para reutilizar funcoes compartilhadas, desde que continuem funcionando quando chamadas isoladamente.

## Implementacao Atual

### Marco 1 - Pre-validacao 00 a 11

Criar estrutura inicial da pasta `execucoes_pipeline` e refatorar as etapas 00 a 11 para exporem funcoes reutilizaveis:

```text
00 verificacao campos
01 limpeza
02 contratacao
03 nota
04 classificacao
05 local editado
06 ajustes finais
07 operadora
08 meta
09 resultado unidade
10 status unidade
11 analise dados
```

Este marco gera todos os resumos da pre-validacao em:

```text
saida_resumo_avaliacoes/pipeline_pre_validacao/
saida_resumo_negativas/pipeline_pre_validacao/
```

e o CSV final completo da pre-validacao em:

```text
data_exec_indiv/avaliacoes/pipeline_pre_validacao_base_final.csv
data_exec_indiv/negativas/pipeline_pre_validacao_base_final.csv
```

Tambem gera o Excel de analise em:

```text
saida_resumo_avaliacoes/pipeline_pre_validacao/exec_11_analise_dados/exec_11_analise_dados.xlsx
```

Implementacao deste marco:

- `exec_00_verificacao_campos.py` expoe `processar_verificacao_campos` e `salvar_resumos_verificacao_campos`;
- `exec_01_limpeza.py` expoe `processar_limpeza` e `salvar_resumos_limpeza`;
- `exec_02_contratacao.py` expoe `processar_contratacao` e `salvar_resumos_contratacao`;
- `exec_03_nota.py` expoe `processar_nota` e `salvar_resumos_nota`;
- `exec_04_classificacao.py` expoe `processar_classificacao` e `salvar_resumos_classificacao`;
- `exec_05_local_editado.py` expoe `processar_local_editado` e `salvar_resumos_local_editado`;
- `exec_06_ajustes_finais.py` expoe `processar_ajustes_finais` e `salvar_resumos_ajustes_finais`;
- `exec_07_operadora.py` expoe `processar_operadora` e `salvar_resumos_operadora`;
- `exec_08_meta.py` expoe `processar_meta` e `salvar_resumos_meta`;
- `exec_09_resultado_unidade.py` expoe `processar_resultado_unidade` e `salvar_resumos_resultado_unidade`;
- `exec_10_status_unidade.py` expoe `processar_status_unidade` e `salvar_resumos_status_unidade`;
- `exec_11_analise_dados.py` expoe `processar_analise_dados` e `salvar_analise_dados`;
- `exec_pre_validacao.py` importa essas funcoes e nao duplica a regra.

### Marcos Seguintes

Criar a execucao final reaproveitando a mesma base da pre-validacao e acrescentando:

```text
12 power bi
13 separacao tipo excel
```

Cada marco deve comparar contagens e resumos contra as execucoes individuais antes de substituir o uso operacional.
