import sys
from pathlib import Path
from io import StringIO
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent / "src"))
from data_loader import DataLoader

BASE_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = BASE_DIR / "data"

# 正常なファイルパス
YAML_PATH_OK = TEST_DATA_DIR / "test_rule_ok.yaml"
CSV_PATH_OK = TEST_DATA_DIR / "test_sample_ok.csv"

# 不正なファイルパス (必要に応じて作成)
CSV_PATH_ERROR = TEST_DATA_DIR / "test_sample_error.csv"

def run_manual_tests():
    print("単体テスト")
    try:
        loader = DataLoader(csv_path=CSV_PATH_OK,yaml_path=YAML_PATH_OK)
        loader.conn.close()

        df_validated = loader.load_data()

        dtype_ok = True

# 検証 B: データ型
        all_int_ok = all(str(df_validated[col].dtype).startswith('int') for col in df_validated.columns)
        print(f"  - データ型チェック (全てint型): {'OK' if all_int_ok else 'NG'}")

    except Exception as e:
        print({e})

if __name__ == "__main__":
    if not TEST_DATA_DIR.is_dir():
        print(f"{TEST_DATA_DIR}がないよ")
    if not CSV_PATH_OK.is_file():
        print(f"{CSV_PATH_OK.name}が{TEST_DATA_DIR.name}内にありません。作成してください。")
        sys.exit(1)
    run_manual_tests()