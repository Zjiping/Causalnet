import pickle
import csv
from pathlib import Path

def merge_multi_pkl_to_csv(pkl_files, csv_path='merged_events.csv'):
    """
    将多个独立的 PKL 文件（每个文件仅含一个 split）合并为单一 CSV，
    并保证全局时间戳严格单调递增。
    """
    global_current_time = 0.0
    total_events = 0

    # 打开 CSV 准备流式写入（不占用大量内存）
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['alarm_id', 'device_id', 'start_timestamp', 'end_timestamp'])

        for pkl_path in pkl_files:
            pkl_path = Path(pkl_path)
            if not pkl_path.exists():
                print(f"⚠️ 文件不存在，跳过: {pkl_path}")
                continue

            print(f"📦 正在读取: {pkl_path.name}")
            with open(pkl_path, 'rb') as f_pkl:
                data = pickle.load(f_pkl)

            # 自动提取唯一的 key 对应的序列列表（兼容 dict 格式）
            if isinstance(data, dict):
                sequences = next(iter(data.values()))
            else:
                sequences = data  # 若 PKL 直接存的是 list 则兼容

            print(f"   ↳ 包含 {len(sequences)} 个序列")

            for seq in sequences:
                for event in seq:
                    alarm_id = event["type_event"]
                    # 严格遵循原逻辑：时间间隔 + 极小值
                    delta = event["time_since_last_event"] + 1e-8

                    # 字段映射：
                    # start_timestamp <- 全局累加时间 (等价于全局拼接后的 time_seqs)
                    # end_timestamp   <- start + delta    (等价于全局拼接后的 time_seqs + time_delta_seqs)
                    start_timestamp = global_current_time
                    end_timestamp = start_timestamp + delta

                    # 写入单行
                    writer.writerow([alarm_id, 0, start_timestamp, end_timestamp])

                    # 更新全局时间，确保下一条记录的时间严格大于当前记录
                    global_current_time = end_timestamp
                    total_events += 1

    print(f"\n✅ 转换完成！共处理 {total_events} 条事件记录。")
    print(f"📄 结果已保存至: {csv_path}")


# ================= 使用示例 =================
if __name__ == "__main__":
    # 按您希望的拼接顺序排列文件路径
    PKL_FILES = [
        "/data/data/final_1024/Kunlun/train.pkl",
        "/data/data/final_1024/Kunlun/train.pkl",
        "/data/data/final_1024/Kunlun/train.pkl",  # 如有更多文件可继续添加
    ]
    
    OUTPUT_CSV = "/data/data/final_1024/Kunlun/merge.csv"
    
    merge_multi_pkl_to_csv(PKL_FILES, csv_path=OUTPUT_CSV)