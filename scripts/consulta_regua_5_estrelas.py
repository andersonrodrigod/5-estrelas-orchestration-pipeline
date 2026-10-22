"""Apura maio-julho/2026 na tabela configurada. Saidas agregadas, sem dados pessoais.

Usa scripts/.env.oracle. Nao aplica SQL_WHERE nem executa o pipeline.
Notas 1-5 sao identificadas como avaliadas; nao presumem conclusao do questionario.
"""
import csv
import json
from datetime import datetime
from pathlib import Path

import oracledb
import consulta_quantidades_oracle_5_estrelas as config


TABELA = config.validar_nome_tabela(config.TABLE_NAME)
PASTA = Path('data/oracle/regua_5_estrelas_2026_maio_julho')


def expressao_data(nome, tipo):
    if tipo in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP,
                oracledb.DB_TYPE_TIMESTAMP_TZ, oracledb.DB_TYPE_TIMESTAMP_LTZ):
        return f'CAST({nome} AS DATE)'
    return f"COALESCE(TO_DATE(TRIM({nome}) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(TRIM({nome}) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'))"


def montar_sql(tipos):
    atendimento = expressao_data('DATA_ATENDIMENTO', tipos['DATA_ATENDIMENTO'])
    resposta = expressao_data('DT_RESPOSTA', tipos['DT_RESPOSTA'])
    base = f"""WITH datas AS (
      SELECT CDATENDIMENTO, CDEMPRESA, NUM_BENEFICIARIO,
        UPPER(TRIM(NOTA1)) AS NOTA, STATUS,
        {atendimento} AS DT_ATEND, {resposta} AS DT_RESP
      FROM {TABELA}
    ), periodo AS (
      SELECT d.*, TO_CHAR(DT_ATEND, 'YYYY-MM') AS MES_ATENDIMENTO,
        TRUNC(DT_RESP) - TRUNC(DT_ATEND) AS DIAS,
        CASE WHEN NOTA IN ('1','2','3','4','5') THEN 'AVALIADA_1_A_5'
             WHEN NOTA IN ('NQA','N/A','IGN') THEN NOTA
             WHEN NOTA IS NULL THEN 'SEM_NOTA' ELSE 'OUTROS' END AS CATEGORIA
      FROM datas d
      WHERE DT_ATEND >= DATE '2026-05-01' AND DT_ATEND < DATE '2026-08-01'
    ) """
    return {
        'resumo_categorias': base + """
          SELECT MES_ATENDIMENTO, COUNT(*) AS TOTAL_REGISTROS,
            SUM(CASE WHEN CATEGORIA='AVALIADA_1_A_5' THEN 1 ELSE 0 END) AS AVALIADAS_1_A_5,
            SUM(CASE WHEN CATEGORIA='NQA' THEN 1 ELSE 0 END) AS NQA,
            SUM(CASE WHEN CATEGORIA='N/A' THEN 1 ELSE 0 END) AS NA,
            SUM(CASE WHEN CATEGORIA='IGN' THEN 1 ELSE 0 END) AS IGN,
            SUM(CASE WHEN CATEGORIA='SEM_NOTA' THEN 1 ELSE 0 END) AS SEM_NOTA,
            SUM(CASE WHEN CATEGORIA='OUTROS' THEN 1 ELSE 0 END) AS OUTROS
          FROM periodo GROUP BY MES_ATENDIMENTO ORDER BY MES_ATENDIMENTO""",
        'faixas_por_categoria': base + """
          SELECT MES_ATENDIMENTO, CATEGORIA, COUNT(*) AS TOTAL,
            SUM(CASE WHEN DIAS BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS DIAS_0_A_6,
            SUM(CASE WHEN DIAS BETWEEN 7 AND 13 THEN 1 ELSE 0 END) AS DIAS_7_A_13,
            SUM(CASE WHEN DIAS BETWEEN 14 AND 20 THEN 1 ELSE 0 END) AS DIAS_14_A_20,
            SUM(CASE WHEN DIAS BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS DIAS_21_A_30,
            SUM(CASE WHEN DIAS > 30 THEN 1 ELSE 0 END) AS ACIMA_30,
            SUM(CASE WHEN DT_RESP < DT_ATEND THEN 1 ELSE 0 END) AS RESPOSTA_ANTERIOR_AO_ATEND,
            SUM(CASE WHEN DIAS < 0 THEN 1 ELSE 0 END) AS DIAS_NEGATIVOS,
            SUM(CASE WHEN DT_RESP IS NULL THEN 1 ELSE 0 END) AS SEM_DATA_RESPOSTA
          FROM periodo GROUP BY MES_ATENDIMENTO, CATEGORIA
          ORDER BY MES_ATENDIMENTO, CATEGORIA""",
        'notas_status': base + """
          SELECT MES_ATENDIMENTO, NOTA, STATUS, COUNT(*) AS QUANTIDADE
          FROM periodo GROUP BY MES_ATENDIMENTO, NOTA, STATUS
          ORDER BY MES_ATENDIMENTO, NOTA, STATUS""",
        'conferencia_chave_candidata': base + """
          SELECT MES_ATENDIMENTO, SUM(QTD) AS TOTAL_REGISTROS,
            COUNT(*) AS GRUPOS_CHAVE_CANDIDATA,
            SUM(CASE WHEN QTD > 1 THEN 1 ELSE 0 END) AS GRUPOS_REPETIDOS,
            SUM(QTD-1) AS LINHAS_EXCEDENTES,
            SUM(CASE WHEN CHAVE_INCOMPLETA=1 THEN QTD ELSE 0 END) AS REGISTROS_CHAVE_INCOMPLETA
          FROM (
            SELECT MES_ATENDIMENTO, CDATENDIMENTO, CDEMPRESA, NUM_BENEFICIARIO,
              DT_ATEND, COUNT(*) AS QTD,
              CASE WHEN CDATENDIMENTO IS NULL OR CDEMPRESA IS NULL
                   OR NUM_BENEFICIARIO IS NULL THEN 1 ELSE 0 END AS CHAVE_INCOMPLETA
            FROM periodo GROUP BY MES_ATENDIMENTO, CDATENDIMENTO, CDEMPRESA,
              NUM_BENEFICIARIO, DT_ATEND
          ) GROUP BY MES_ATENDIMENTO ORDER BY MES_ATENDIMENTO""",
    }


def main():
    # O Oracle Client deve resolver o alias sem reserializacao por ConnectParams:
    # essa conversao pode descartar SOURCE_ROUTE, exigido pelo proxy corporativo.
    oracledb.init_oracle_client(
        lib_dir=config.ORACLE_CLIENT_DIR, config_dir=config.TNS_ADMIN
    )
    if not config.ORACLE_PASSWORD:
        raise ValueError('Configure ORACLE_PASSWORD no ambiente ou .env.oracle.')
    print('Conectando ao Oracle...', flush=True)
    with oracledb.connect(user=config.ORACLE_USER, password=config.ORACLE_PASSWORD,
                          dsn=config.montar_dsn()) as conn:
        conn.call_timeout = 600000
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM {TABELA} WHERE 1=0')
            tipos = {d[0]: d[1] for d in cur.description}
            consultas = montar_sql(tipos)
            pasta_execucao = PASTA / datetime.now().strftime('%Y%m%d_%H%M%S')
            pasta_execucao.mkdir(parents=True, exist_ok=False)
            resultado = {}
            for nome, sql in consultas.items():
                (pasta_execucao / f'{nome}.sql').write_text(sql, encoding='utf-8')
                print(f'Executando {nome}...', flush=True)
                cur.execute(sql)
                colunas = [d[0] for d in cur.description]
                linhas = cur.fetchall()  # Somente agregados.
                with (pasta_execucao / f'{nome}.csv').open('w', encoding='utf-8-sig', newline='') as f:
                    w = csv.writer(f, delimiter=';')
                    w.writerow(colunas)
                    w.writerows(linhas)
                resultado[nome] = [dict(zip(colunas, r)) for r in linhas]
            (pasta_execucao / 'resultados.json').write_text(
                json.dumps(resultado, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
            (pasta_execucao / 'criterios.txt').write_text(
                'Origem: ' + TABELA + '\n'
                'Periodo: DATA_ATENDIMENTO >= 01/05/2026 e < 01/08/2026. Sem operadora.\n'
                'Total inclui todas as notas, inclusive nulas. Contagem de registros sem deduplicacao automatica.\n'
                'Chave candidata: CDATENDIMENTO + CDEMPRESA + NUM_BENEFICIARIO + DATA_ATENDIMENTO.\n'
                'Notas de 1 a 5 indicam avaliacao. Conclusao do questionario requer validacao da regra de negocio.\n'
                'NQA usa DT_RESPOSTA como data do evento, sujeito a confirmacao na origem.\n'
                'Faixas: diferenca entre datas sem horario. Nulos, acima de 30 e negativos aparecem separadamente.\n'
                'RESPOSTA_ANTERIOR_AO_ATEND e diagnostico adicional e pode sobrepor a faixa de zero dias.\n'
                'Datas de atendimento invalidas ou vazias nao podem ser atribuidas ao periodo.\n', encoding='utf-8')
            print(f'Finalizado: {pasta_execucao}', flush=True)


if __name__ == '__main__':
    main()
