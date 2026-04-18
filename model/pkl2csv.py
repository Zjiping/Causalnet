import pickle
import csv
from itertools import accumulate

def pkl_to_csv(pkl_path, split='train', csv_path='output.csv'):
    # 1. 读取 pkl 文件
    with open(pkl_path, 'rb') as f:
        data = pickle.load(f)
        
    # 2. 获取指定划分的数据序列
    sequences = data[split]
    
    # 3. 写入 CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(['alarm_id', 'device_id', 'start_timestamp', 'end_timestamp'])
        
        for seq in sequences:
            # 提取原始字段
            type_seq = [x["type_event"] for x in seq]
            time_delta_seq = [x["time_since_last_event"] + 1e-8 for x in seq]
            
            # 计算 time_seqs (累积时间)
            time_seq = list(accumulate(time_delta_seq))
            
            # 严格按您的要求映射：
            # start_timestamp <- time_seqs
            # end_timestamp   <- time_seqs + time_delta_seqs
            start_timestamps = time_seq
            end_timestamps = [t + dt for t, dt in zip(time_seq, time_delta_seq)]
            
            # 逐条写入
            for alarm_id, start, end in zip(type_seq, start_timestamps, end_timestamps):
                writer.writerow([alarm_id, 0, start, end])
                
    print(f"✅ 转换完成，共处理 {sum(len(s) for s in sequences)} 条事件记录，已保存至: {csv_path}")

# ================= 使用示例 =================
if __name__ == "__main__":
    # 替换为您的实际路径和 split 名称（如 'train', 'val', 'test' 等）
    PKL_PATH = "your_data.pkl"
    SPLIT_NAME = "train"  
    CSV_PATH = "output.csv"
    
    pkl_to_csv(PKL_PATH, split=SPLIT_NAME, csv_path=CSV_PATH)