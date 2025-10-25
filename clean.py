import pandas as pd
import numpy as np
import os

# 文件夹路径
folder_path = r"/flows_csv"  # 存放 CSV 文件的文件夹路径

# 获取文件夹中的所有 .csv 文件
all_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 遍历每个 CSV 文件，进行清洗并保存
for file in all_files:
    file_path = os.path.join(folder_path, file)

    # 读取 CSV 文件，增加容错编码
    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    print(f"正在处理文件: {file}, 原始数据大小: {df.shape}")

    # ---------------------------------------
    # 1. 删除完全重复的行
    # ---------------------------------------
    df.drop_duplicates(inplace=True)

    # ---------------------------------------
    # 2. 删除空值过多的列（缺失率 > 50%）
    # ---------------------------------------
    missing_ratio = df.isnull().mean()
    cols_to_drop = missing_ratio[missing_ratio > 0.5].index
    df.drop(columns=cols_to_drop, inplace=True)
    print(f"删除空值过多的列: {list(cols_to_drop)}")

    # ---------------------------------------
    # 3. 删除无意义列（全为同一个值的列）
    # ---------------------------------------
    constant_cols = [col for col in df.columns if df[col].nunique() == 1]
    df.drop(columns=constant_cols, inplace=True)
    print(f"删除无变化列: {constant_cols}")

    # ---------------------------------------
    # 4. 尝试将数值列转换为 float 类型（过滤非数值）
    # ---------------------------------------
    for col in df.columns:
        try:
            df[col] = df[col].astype(float)
        except:
            pass  # 非数值列跳过

    # ---------------------------------------
    # 5. 处理异常值：删除负值列（如存在）
    # ---------------------------------------
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df = df[df[col] >= 0]

    # ---------------------------------------
    # 6. 删除所有仍然为空的行
    # ---------------------------------------
    df.dropna(inplace=True)

    # ---------------------------------------
    # 7. 保存清洗后的文件
    # ---------------------------------------
    clean_file_name = file.replace('.csv', '_clean.csv')  # 生成清洗后的文件名
    clean_file_path = os.path.join(folder_path, clean_file_name)
    df.to_csv(clean_file_path, index=False, encoding='utf-8')
    print(f"✅ 清洗完成！保存到: {clean_file_path}")
    print(f"清洗后数据大小: {df.shape}")
