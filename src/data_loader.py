import yaml
import pandas as pd
from pydantic import BaseModel, ValidationError, create_model
from typing import Any
from Settings import settings
import pymysql

class DataLoader:
    def __init__(self, csv_path, yaml_path):
        self.csv_path = csv_path
        self.yaml_path = yaml_path
        self.rules = self.load_rules_features()  # {"ブログ更新": int, ...}
        self.conn = self.connect_db()

    def connect_db(self):
        return pymysql.connect(
            host='127.0.0.1',
            port=3307,
            user='root',
            password='',
            database='mysql',
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            unix_socket='/tmp/mysql_custom.sock'
        )
    
    def create_table(self, table_name: str):
        self.table_name = table_name  # インスタンスに保存しておく
        cursor = self.conn.cursor()
        cols_sql = []

        for col, dtype in self.rules.items():
            sql_type = self._map_sql_type(dtype)
            cols_sql.append(f"{col} {sql_type}") 

        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(cols_sql)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        cursor.execute(sql)
        self.conn.commit()
        cursor.close()
        print(f"テーブル {self.table_name} を作成しました。")

    def insert_data(self,table_name,df):
        cursor = self.conn.cursor()
        cols = df.columns.tolist()
        
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO `{table_name}` ({', '.join(cols)}) VALUES ({placeholders})"

        for row in df.to_dict(orient="records"):
            cursor.execute(sql, tuple(row[col] for col in cols))
    
        self.conn.commit()
        cursor.close()
        print(f"{len(df)} 件のデータを `{table_name}` に挿入しました。")

    def load_rules(self):
        with open(self.yaml_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def load_rules_features(self) -> dict[str, Any]:
        tmp = self.load_rules()
        features_list = tmp["features"]
# [{'ブログ更新': 'int'}, {'試験勉強': 'int'}, {'コーディング': 'int'}, {'環境構築': 'int'}, {'読書': 'int'}, {'1日の幸福度': 'int'}]
        rules_dict = {}
        for d in features_list:
            for k, v in d.items():
                rules_dict[k] = v
# {'ブログ更新': 'int', '試験勉強': 'int', 'コーディング': 'int', '環境構築': 'int', '読書': 'int', '1日の幸福度': 'int'}
        return rules_dict

    def load_data(self):
        df = pd.read_csv(self.csv_path)
        df = self.validate_and_convert(df)
        return df

    def validate_and_convert(self, df):
        fields = {}
        # rules の全列を Pydantic モデルに渡す
        for col, dtype in self.rules.items():
            py_type = self._map_dtype(dtype)
            #print(dtype) #int,int,int
            fields[col] = (py_type, ...)
# {'ブログ更新': (<class 'int'>, Ellipsis), '試験勉強': (<class 'int'>, Ellipsis), 'コーディング': (<class 'int'>, Ellipsis), '環境構築': (<class 'int'>, Ellipsis), '読書': (<class 'int'>, Ellipsis), '1日の幸福度': (<class 'int'>, Ellipsis)}

        DynamicModel = create_model("DynamicModel", **fields) # タプルになってる辞書をキーワード引数に展開
        # <class 'data_loader.DynamicModel'>

        validated_rows = []
        for row in df.to_dict(orient="records"):
            try:
                validated = DynamicModel(**row)
                validated_rows.append(validated.dict())
# ブログ更新=1 試験勉強=0 コーディング=1 環境構築=0 読書=1 1日の幸福度=1
# ブログ更新=1 試験勉強=1 コーディング=1 環境構築=0 読書=0 1日の幸福度=0
# ブログ更新=0 試験勉強=0 コーディング=1 環境構築=0 読書=0 1日の幸福度=0
            except ValidationError as e:
                print(f"Validation error for row {row}: {e}")
                
        return pd.DataFrame(validated_rows)

    def _map_dtype(self, dtype_info):
        if isinstance(dtype_info, dict):
            dtype_str = dtype_info.get("type", "str")
        elif isinstance(dtype_info, list):
            dtype_str = dtype_info[0]
        else:
            dtype_str = dtype_info
        mapping = {"int": int, "float": float, "str": str, "bool": bool}
        return mapping.get(dtype_str, str)

    def get_dataframe(self):
        return self.load_data()

    def _map_sql_type(self, dtype):
        mapping = {
            "int": "INT",
            "float": "FLOAT",
            "str": "VARCHAR(255)",
            "bool": "TINYINT(1)"
        }
        return mapping.get(dtype, "VARCHAR(255)")

