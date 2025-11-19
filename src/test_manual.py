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

def get_pandas_prefix_from_yaml_type(yaml_type_str: str) -> str:
    type_lower = yaml_type_str.lower()
    if type_lower in ('int','int64','bool'):
        return 'int'

    elif type_lower in ('float','double'):
        return 'float'
    else:
        return 'object'

def run_manual_tests():
    try:
        loader = DataLoader(csv_path=CSV_PATH_OK,yaml_path=YAML_PATH_OK)
        loader.conn.close()

        df_validated = loader.load_data()
        # print(df_validated)
        """
ブログ更新  試験勉強  コーディング  環境構築   読書  1日の幸福度
0       1   0.0       1     0  1.0       1
1       1   1.0       1     0  0.0       0
2       0   0.0       1     0  0.0       0
3       0   1.0       1     0  1.0       0
4       1   0.0       1     0  0.0       1
5       1   1.0       0     0  0.0       0
6       0   1.0       1     0  0.0       1
        """
        
        EXPCTED_YAML_RULES = loader.rules
        # print(EXPCTED_YAML_RULES)
        # {'ブログ更新': 'int', '試験勉強': 'float', 'コーディング': 'int', '環境構築': 'int', '読書': 'float', '1日の幸福度': 'int'}

        dtype_ok = True

        for col in df_validated.columns:
            excepted_yaml_type = EXPCTED_YAML_RULES.get(col)
            # print(EXPCTED_YAML_RULES.get(col))
            """"
            int
            float
            int
            int
            float
            int
            """
            if excepted_yaml_type is None:
                continue
            expected_prefix = get_pandas_prefix_from_yaml_type(excepted_yaml_type)
            # print(expected_prefix)
            """
            int
            float
            int
            int
            float
            int
            """
            actual_dtype = str(df_validated[col].dtype)    
            # print(actual_dtype)
            """
            int64
            float64
            int64
            int64
            float64
            int64
            """
            if not actual_dtype.startswith(expected_prefix):
                dtype_ok = False  
                print(f"    -> NG: 列 '{col}' の型が不一致です。期待値='{expected_prefix}', 実際='{actual_dtype}'")
            else:
                dtype_ok = True
                print(f"    -> OK: {col}, {actual_dtype}")        
        print(f"  - 総合判定: {'OK' if dtype_ok else 'NG'}")

    except Exception as e:
        print({e})

if __name__ == "__main__":
    if not TEST_DATA_DIR.is_dir():
        print(f"{TEST_DATA_DIR}がないよ")
    if not CSV_PATH_OK.is_file():
        print(f"{CSV_PATH_OK.name}が{TEST_DATA_DIR.name}内にありません。作成してください。")
        sys.exit(1)

    if not CSV_PATH_OK.is_file() or not YAML_PATH_OK.is_file():
        if not CSV_PATH_OK.is_file():
            print(f"エラー: {CSV_PATH_OK.name} が {TEST_DATA_DIR.name} 内にありません。作成してください。")
        if not YAML_PATH_OK.is_file():
            print(f"エラー: {YAML_PATH_OK.name} が {TEST_DATA_DIR.name} 内にありません。作成してください。")
        sys.exit(1)
        
    run_manual_tests()

    #終わり!