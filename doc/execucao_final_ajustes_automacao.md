# Execucao Final de Ajustes da Automacao

## Objetivo

Criar uma etapa final para aplicar ajustes pontuais na base sem precisar alterar os insumos principais nem reexecutar toda a automacao desde o inicio.

A ideia e ter uma execucao propria para os ultimos ajustes conhecidos:

```text
exec_06_ajustes_finais.py
```

Com isso, a etapa atual de operadora passa a ser `exec_07_operadora.py`, e os calculos seguintes tambem sao empurrados.

```text
exec_14_power_bi.py
```

## Por Que Criar Essa Etapa

Alguns ajustes sao excecoes de negocio e nao devem ficar misturados nos cadastros principais, porque podem depender de combinacoes especificas como:

```text
LOCAL + UF
LOCAL + CONTRATACAO
LOCAL + CLASSIFICACAO
LOCAL + OPERADORA
```

Quando esses ajustes ficam direto no insumo base, o risco e aplicar uma correcao ampla demais.

Exemplo:

```text
NOTRE DAME INTERMEDICA SAUDE + UF SP -> HOSPITAL SAO LUCAS
```

Se isso for colocado apenas como `LOCAL -> LOCAL EDITADO`, o codigo atual aplica pelo nome do local e ignora a UF.

## Formato Sugerido Para Regras

Criar uma planilha ou CSV separado:

```text
utils/insumos/ajustes_finais.xlsx
```

Aba sugerida:

```text
ajustes_local_editado
```

Colunas sugeridas:

```text
STATUS_ATIVO
ORDEM
COLUNA_AJUSTAR
VALOR_NOVO
VALOR_ATUAL_ESPERADO
PERMITIR_SOBRESCRITA
COLUNA_FILTRO_1
COMPARADOR_1
VALOR_1
COLUNA_FILTRO_2
COMPARADOR_2
VALOR_2
COLUNA_FILTRO_3
COMPARADOR_3
VALOR_3
DESCRICAO
```

Exemplo de regra:

```text
STATUS_ATIVO = sim
ORDEM = 1
COLUNA_AJUSTAR = LOCAL EDITADO
VALOR_NOVO = HOSPITAL SAO LUCAS
VALOR_ATUAL_ESPERADO =
PERMITIR_SOBRESCRITA = sim
COLUNA_FILTRO_1 = LOCAL
COMPARADOR_1 = em_lista
VALOR_1 = NOTRE DAME INTERMEDICA SAUDE SA; NOTRE DAME INTERMEDICA SAUDE S A; NOTRE DAME INTERMEDICA SAUDE S.A.; NOTRE DAME INTERMEDICA SAUDE S.A; NOTRE DAME INTERMEDICA SAUDE S; NOTRE DAME INTERMEDICA SAUDE
COLUNA_FILTRO_2 = UF
COMPARADOR_2 = igual
VALOR_2 = SP
DESCRICAO = Ajusta Notre Dame SP para Hospital Sao Lucas
```

Nao e necessario usar `APLICAR_SOMENTE_VAZIO`, porque essa etapa final deve atuar sobre uma base ja completa. Para evitar alteracoes amplas por engano, usar `VALOR_ATUAL_ESPERADO` quando a regra depender do valor atual da coluna.

Exemplo:

```text
COLUNA_AJUSTAR = LOCAL EDITADO
VALOR_ATUAL_ESPERADO = NOTRE DAME INTERMEDICA SAUDE S A
VALOR_NOVO = HOSPITAL SAO LUCAS
```

## Campos Que Precisam Ser Recalculados

Se a execucao final alterar `LOCAL EDITADO`, ela precisa atualizar os campos que dependem dele:

```text
OPERADORA
META
RESULTADO DA UNIDADE
STATUS UNIDADE
```

Ordem recomendada dentro da execucao final:

```text
1. Ler a base da execucao 05, ja com LOCAL EDITADO inicial.
2. Aplicar ajustes ativos na ordem definida.
3. Registrar auditoria de linhas alteradas.
4. Salvar base ajustada.
5. Deixar OPERADORA, META, RESULTADO DA UNIDADE e STATUS UNIDADE para as execucoes seguintes.
```

## Saidas Recomendadas

Base final ajustada:

```text
data_exec_indiv/avaliacoes/06_base_com_ajustes_finais.csv
```

Auditoria:

```text
saida_resumo_avaliacoes/exec_06_ajustes_finais/exec_06_ajustes_finais_auditoria.csv
```

Resumo:

```text
saida_resumo_avaliacoes/exec_06_ajustes_finais/exec_06_ajustes_finais_resumo.json
saida_resumo_avaliacoes/exec_06_ajustes_finais/exec_06_ajustes_finais_resumo.txt
```

## Auditoria Obrigatoria

Cada linha alterada deve registrar:

```text
ORDEM_REGRA
DESCRICAO
COLUNA_AJUSTAR
VALOR_ANTERIOR
VALOR_NOVO
LOCAL
UF
CLASSIFICACAO
LOCAL EDITADO ANTES
LOCAL EDITADO DEPOIS
OPERADORA ANTES
OPERADORA DEPOIS
META ANTES
META DEPOIS
RESULTADO DA UNIDADE ANTES
RESULTADO DA UNIDADE DEPOIS
STATUS UNIDADE ANTES
STATUS UNIDADE DEPOIS
```

## Beneficio

Com essa etapa, os ajustes finais ficam:

```text
rastreaveis
reversiveis
controlados por regra
sem gambiarra no insumo principal
sem necessidade de reprocessar toda a base desde a limpeza
```
