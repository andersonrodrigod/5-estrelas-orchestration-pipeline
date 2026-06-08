# Ajuste Notre Dame para Hospital Sao Lucas

## Problema

Hoje o ajuste esta dentro da aba `insumos` do arquivo `utils/insumos/insumos 5 estrelas.xlsx`, alterando o `local editado` de alguns registros Notre Dame para `HOSPITAL SAO LUCAS`.

O problema e que a execucao `exec_05_local_editado.py` monta o mapa usando apenas a coluna `Local`:

```python
mapa_local_editado = df_insumos.set_index('LOCAL_COMPARACAO')['local editado']
```

Ou seja, mesmo que a planilha tenha `UF = SP`, a regra aplicada no codigo nao considera a UF. Se aparecer o mesmo `LOCAL` em outra UF, ele tambem pode ser alterado indevidamente.

## Regra Atual Encontrada

Na planilha atual existem mapeamentos como:

```text
NOTRE DAME INTERMEDICA SAUDE SA     | SP | HOSPITAL SAO LUCAS
NOTRE DAME INTERMEDICA SAUDE S A    | SP | HOSPITAL SAO LUCAS
NOTRE DAME INTERMEDICA SAUDE S.A.   | SP | HOSPITAL SAO LUCAS
NOTRE DAME INTERMEDICA SAUDE S.A    | SP | HOSPITAL SAO LUCAS
NOTRE DAME INTERMEDICA SAUDE S      | SP | HOSPITAL SAO LUCAS
NOTRE DAME INTERMEDICA SAUDE        | SP | HOSPITAL SAO LUCAS
```

Essas regras nao devem ficar como simples alteracao no insumo principal, porque parecem um ajuste especifico de excecao.

## Regra Correta

Criar uma regra de ajuste que considere pelo menos:

```text
UF = SP
LOCAL = uma das variacoes Notre Dame abaixo
LOCAL EDITADO NOVO = HOSPITAL SAO LUCAS
```

Variacoes inicialmente previstas:

```text
NOTRE DAME INTERMEDICA SAUDE SA
NOTRE DAME INTERMEDICA SAUDE S A
NOTRE DAME INTERMEDICA SAUDE S.A.
NOTRE DAME INTERMEDICA SAUDE S.A
NOTRE DAME INTERMEDICA SAUDE S
NOTRE DAME INTERMEDICA SAUDE
```

## Recomendacao

Remover esse ajuste do `insumos 5 estrelas.xlsx` e mover para uma etapa propria de ajustes finais.

Essa etapa deve rodar depois que a base ja tiver:

```text
CLASSIFICACAO
LOCAL EDITADO
OPERADORA
META
RESULTADO DA UNIDADE
STATUS UNIDADE
```

Quando alterar `LOCAL EDITADO`, a etapa tambem deve recalcular os campos dependentes:

```text
OPERADORA
META
RESULTADO DA UNIDADE
STATUS UNIDADE
```

Assim o ajuste fica rastreavel, especifico e nao vira uma correcao escondida no insumo base.
