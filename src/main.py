from pathlib import Path
from .data_loader import DataLoader
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
import lightgbm as lgb

# 現在のmain.pyファイルの絶対パスを取得し、そのディレクトリを基準にする
BASE_DIR = Path(__file__).resolve().parent

# YAMLファイルとCSVファイルの絶対パスを作成
yaml_file_path = BASE_DIR / "rule.yaml"
csv_file_path = BASE_DIR / "sample.csv"

# DataLoader を初期化する際に、Pathオブジェクト（絶対パス）を渡す
loader = DataLoader(csv_file_path, yaml_file_path)
loader.create_table("my_table")
df = loader.get_dataframe()
loader.insert_data("my_table", df)

# #予測ターゲットの格納
target_df = df[["1日の幸福度"]]

# #特徴量の格納（説明変数：B列以降）
train_df = df[["ブログ更新","試験勉強", "コーディング", "環境構築", "読書"]]

# #モデル学習のための、訓練データとテストデータを7:3で分割
X_train, X_test, y_train, y_test = train_test_split(train_df, target_df, test_size=0.3)

# #XGBoostで学習するためのデータ形式に変換
train_data = lgb.Dataset(X_train, label=y_train)
test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

# ハイパーパラメータ設定
params = {
    'objective': 'binary',
    'metric': 'binary_error',
    'boosting_type': 'gbdt',
    'learning_rate': 0.1,
    'num_leaves': 31,
    'verbose': -1
}

# モデルパラメータ設定
model = lgb.train(params, train_data, valid_sets=[test_data], num_boost_round=100)

# 予測
y_pred_prob = model.predict(X_test)
y_pred = [1 if p > 0.5 else 0 for p in y_pred_prob]

accuracy = accuracy_score(y_test, y_pred)
cm = confusion_matrix(y_test, y_pred)

# モデルが正しく予測できたデータの割合
print("Accuracy:", accuracy)
#混同行列
print("Confusion Matrix:\n", cm)
