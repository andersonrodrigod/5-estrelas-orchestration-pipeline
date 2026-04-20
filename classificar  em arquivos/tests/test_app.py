import unittest
from pathlib import Path
from unittest.mock import patch
import shutil
import uuid

import pandas as pd

import app


def write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def make_test_dir() -> Path:
    base_dir = Path(__file__).resolve().parent / "_artifacts"
    test_dir = base_dir / uuid.uuid4().hex
    test_dir.mkdir(parents=True, exist_ok=True)
    return test_dir


class AppExcelTests(unittest.TestCase):
    def test_load_all_sheets_concatena_todas_as_abas(self) -> None:
        tmp_dir = make_test_dir()
        try:
            file_path = tmp_dir / "teste.xlsx"
            write_workbook(
                file_path,
                {
                    "aba 1": pd.DataFrame(
                        {
                            "CLASSIFICACAO": ["LABORATORIO"],
                            "valor": [1],
                        }
                    ),
                    "aba 2": pd.DataFrame(
                        {
                            "CLASSIFICACAO": ["HAPCLINICA"],
                            "valor": [2],
                        }
                    ),
                },
            )

            df = app.load_all_sheets(file_path, file_path.name)

            self.assertEqual(len(df), 2)
            self.assertEqual(
                sorted(df["__origem_aba"].tolist()),
                ["aba 1", "aba 2"],
            )
            self.assertEqual(
                sorted(df["CLASSIFICACAO"].tolist()),
                ["HAPCLINICA", "LABORATORIO"],
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_main_processa_registros_de_todas_as_abas(self) -> None:
        tmp_path = make_test_dir()
        try:
            bi_path = tmp_path / "bi.xlsx"
            neg_path = tmp_path / "neg.xlsx"

            write_workbook(
                bi_path,
                {
                    "tipo 1 a 7": pd.DataFrame(
                        {
                            "CLASSIFICACAO": ["LABORATORIO"],
                            "id": [1],
                        }
                    ),
                    "tipo 8 a 14": pd.DataFrame(
                        {
                            "CLASSIFICACAO": ["HAPCLINICA"],
                            "id": [2],
                        }
                    ),
                },
            )
            write_workbook(
                neg_path,
                {
                    "tipos 1 a 7": pd.DataFrame(
                        {
                            "CLASSIFICAÇÃO": ["TELEMEDICINA"],
                            "id": [3],
                        }
                    ),
                    "tipos 8 a 14": pd.DataFrame(
                        {
                            "CLASSIFICAÇÃO": ["ODONTOLOGIA"],
                            "id": [4],
                        }
                    ),
                },
            )

            with patch.object(app, "INPUT_BI", str(bi_path)), patch.object(
                app, "INPUT_NEGATIVAS", str(neg_path)
            ):
                previous_cwd = Path.cwd()
                try:
                    import os

                    os.chdir(tmp_path)
                    app.main()
                finally:
                    os.chdir(previous_cwd)

            exec_dirs = list((tmp_path / "execucoes").glob("execucao_*"))
            self.assertEqual(len(exec_dirs), 1)

            output_dir = exec_dirs[0]
            diagnostico = pd.read_excel(
                output_dir / f"{app.OUTPUT_PREFIX}diagnostico.xlsx",
                sheet_name="avaliacoes",
            )
            hapclinica = pd.read_excel(
                output_dir / f"{app.OUTPUT_PREFIX}hapclinica.xlsx",
                sheet_name="avaliacoes",
            )
            teleconsulta = pd.read_excel(
                output_dir / f"{app.OUTPUT_PREFIX}teleconsulta.xlsx",
                sheet_name="negativas",
            )
            odontologia = pd.read_excel(
                output_dir / f"{app.OUTPUT_PREFIX}odontologia.xlsx",
                sheet_name="negativas",
            )

            self.assertEqual(diagnostico["CLASSIFICACAO"].tolist(), ["LABORATORIO"])
            self.assertEqual(hapclinica["CLASSIFICACAO"].tolist(), ["HAPCLINICA"])
            self.assertEqual(teleconsulta["CLASSIFICAÇÃO"].tolist(), ["TELEMEDICINA"])
            self.assertEqual(odontologia["CLASSIFICAÇÃO"].tolist(), ["ODONTOLOGIA"])
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
