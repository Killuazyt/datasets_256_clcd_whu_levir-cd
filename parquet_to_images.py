import os
import io
import pandas as pd
from PIL import Image

parquet_path = r"C:\Users\Killua\Desktop\数据集处理\256crop levercd\test-00000-of-00001-31d7c3e3444e5b5d.parquet"
output_root = r"C:\Users\Killua\Desktop\数据集处理\levercd256"
split_name = "test"

dir_A = os.path.join(output_root, split_name, "A")
dir_B = os.path.join(output_root, split_name, "B")
dir_label = os.path.join(output_root, split_name, "label")

os.makedirs(dir_A, exist_ok=True)
os.makedirs(dir_B, exist_ok=True)
os.makedirs(dir_label, exist_ok=True)

df = pd.read_parquet(parquet_path)

print("列名：", df.columns.tolist())
print("样本数：", len(df))

def save_image_from_cell(cell, save_path, force_rgb=False):
    try:
        if isinstance(cell, dict):
            if cell.get("bytes") is not None:
                img = Image.open(io.BytesIO(cell["bytes"]))
            elif cell.get("path") is not None and os.path.exists(cell["path"]):
                img = Image.open(cell["path"])
            else:
                return False
        elif isinstance(cell, bytes):
            img = Image.open(io.BytesIO(cell))
        else:
            return False

        if force_rgb:
            img = img.convert("RGB")

        img.save(save_path)
        return True

    except Exception as e:
        print(f"保存失败: {save_path}, 错误: {e}")
        return False

success_count = 0
fail_count = 0

for i, row in df.iterrows():
    file_name = f"{i:06d}.png"

    save_path_A = os.path.join(dir_A, file_name)
    save_path_B = os.path.join(dir_B, file_name)
    save_path_label = os.path.join(dir_label, file_name)

    ok1 = save_image_from_cell(row["imageA"], save_path_A, force_rgb=True)
    ok2 = save_image_from_cell(row["imageB"], save_path_B, force_rgb=True)
    ok3 = save_image_from_cell(row["label"], save_path_label, force_rgb=False)

    if ok1 and ok2 and ok3:
        success_count += 1
    else:
        fail_count += 1
        print(f"第 {i} 条样本导出异常")

print("\n导出完成")
print(f"成功: {success_count}")
print(f"失败: {fail_count}")
print(f"输出目录: {output_root}")