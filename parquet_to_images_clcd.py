import os
import io
import pandas as pd
from PIL import Image

# =========================
# 1. 三个 parquet 文件路径
# =========================
parquet_files = {
    "test":  r"C:\Users\Killua\Desktop\数据集处理\256 CLCD\test-00000-of-00001-16b08e795600127a.parquet",
    "train": r"C:\Users\Killua\Desktop\数据集处理\256 CLCD\train-00000-of-00001-f4e6400d631fa619.parquet",
    "val":   r"C:\Users\Killua\Desktop\数据集处理\256 CLCD\val-00000-of-00001-3ad7e27abb77f8c4.parquet",
}

# =========================
# 2. 输出根目录
# =========================
output_root = r"C:\Users\Killua\Desktop\数据集处理\clcd256"


def save_image_from_cell(cell, save_path, force_rgb=False):
    """
    兼容常见 parquet 图像存储形式：
    1) dict: {'bytes': ..., 'path': ...}
    2) 直接 bytes
    """
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


def export_one_split(split_name, parquet_path, output_root):
    print(f"\n========== 开始处理 {split_name} ==========")
    print(f"读取文件: {parquet_path}")

    if not os.path.exists(parquet_path):
        print(f"文件不存在: {parquet_path}")
        return 0, 0

    # 输出目录
    dir_A = os.path.join(output_root, split_name, "A")
    dir_B = os.path.join(output_root, split_name, "B")
    dir_label = os.path.join(output_root, split_name, "label")

    os.makedirs(dir_A, exist_ok=True)
    os.makedirs(dir_B, exist_ok=True)
    os.makedirs(dir_label, exist_ok=True)

    # 读取 parquet
    df = pd.read_parquet(parquet_path)

    print("列名：", df.columns.tolist())
    print("样本数：", len(df))

    required_cols = ["imageA", "imageB", "label"]
    for col in required_cols:
        if col not in df.columns:
            print(f"缺少列: {col}")
            return 0, len(df)

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
            print(f"{split_name} 第 {i} 条样本导出异常")

    print(f"{split_name} 导出完成")
    print(f"成功: {success_count}")
    print(f"失败: {fail_count}")

    return success_count, fail_count


def main():
    os.makedirs(output_root, exist_ok=True)

    total_success = 0
    total_fail = 0

    process_order = ["train", "val", "test"]

    for split_name in process_order:
        parquet_path = parquet_files[split_name]
        success_count, fail_count = export_one_split(split_name, parquet_path, output_root)
        total_success += success_count
        total_fail += fail_count

    print("\n========== 全部导出完成 ==========")
    print(f"总成功: {total_success}")
    print(f"总失败: {total_fail}")
    print(f"输出目录: {output_root}")


if __name__ == "__main__":
    main()