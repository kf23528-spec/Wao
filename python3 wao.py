#!/usr/bin/env python3
"""
使い方:
  pip install anvil-parser2 tqdm
  python count_blocks_from_region.py --region-dir /path/to/world/region --out results.json --workers 8

注意:
  - anvil-parser のバージョン差で API 名が微妙に異なる場合があります。
  - 大きな領域を解析する場合は checkpoint の保存先ディスクに十分な空きが必要です。
"""
import os
import sys
import json
import glob
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# anvil-parser をインポート
try:
    import anvil  # anvil-parser2 / anvil-parser
except Exception as e:
    print("ERROR: anvil-parser が見つかりません。pip install anvil-parser2 を実行してください。")
    raise e

CHECKPOINT = "region_checkpoint.json"

def list_region_files(region_dir):
    pats = ["r.*.*.mca", "*.mca"]
    files = []
    for p in pats:
        files += glob.glob(os.path.join(region_dir, p))
    files = sorted(set(files))
    return files

def process_region(path):
    """
    1リージョンファイルを読み、含まれるチャンクのブロックを列挙して種類ごとにカウントして返す。
    (戻り値) (region_path, {block_name: count, ...}, processed_chunk_count)
    """
    counts = Counter()
    processed_chunks = 0
    try:
        # Region を開く（anvil.Region(path) または Region.from_file(path) のどちらか）
        try:
            region = anvil.Region(path)
        except TypeError:
            # もしコンストラクタが異なれば from_file を試す
            region = anvil.Region.from_file(path)
    except Exception as e:
        return (path, {}, 0, f"open-error: {e}")

    # region.iter_chunks() / region.chunks などバージョン差に注意
    try:
        # 安全なイテレータを使う
        for cx, cz in region.iter_chunks():
            try:
                chunk = region.get_chunk(cx, cz)
            except Exception:
                continue
            # chunk.stream_chunk() でブロックを逐次取得（ドキュメント参照）
            try:
                for block in chunk.stream_chunk():
                    # block の識別子: 1.13+ 系なら namespaced name を持つ可能性あり
                    name = None
                    if hasattr(block, "namespaced_name"):
                        name = block.namespaced_name
                    elif hasattr(block, "id"):
                        name = str(block.id)
                    else:
                        # fallback: repr
                        name = repr(block)
                    counts[name] += 1
                processed_chunks += 1
            except Exception:
                # 何かのチャンクで壊れている可能性があるのでスキップ
                continue
    except Exception as e:
        return (path, {}, processed_chunks, f"iter-error: {e}")

    return (path, dict(counts), processed_chunks, None)


def save_checkpoint(acc_counts, processed_regions, outpath):
    data = {
        "counts": acc_counts,
        "processed_regions": list(processed_regions)
    }
    with open(outpath, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def merge_counters(acc, new):
    for k,v in new.items():
        acc[k] = acc.get(k,0) + v

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region-dir", required=True, help="world/region のパス")
    parser.add_argument("--out", default="counts_final.json", help="出力 JSON")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    files = list_region_files(args.region_dir)
    if not files:
        print("region フォルダに .mca が見つかりません。パスを確認してください。")
        sys.exit(1)

    # 既存チェックポイントを読み込み（あれば途中再開）
    processed_regions = set()
    acc_counts = {}
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT,"r",encoding="utf8") as f:
            state = json.load(f)
            processed_regions = set(state.get("processed_regions", []))
            acc_counts = state.get("counts", {})

    to_process = [p for p in files if p not in processed_regions]
    print(f"全リージョン: {len(files)}, 未処理: {len(to_process)}")

    # 並列処理
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_region, p): p for p in to_process}
        for fut in tqdm(as_completed(futures), total=len(futures)):
            try:
                path, new_counts, chunks, err = fut.result()
            except Exception as e:
                print("Worker error:", e)
                continue
            if err:
                print("Warning:", path, err)
            # マージ
            merge_counters(acc_counts, new_counts)
            processed_regions.add(path)
            # 定期的にチェックポイント保存
            save_checkpoint(acc_counts, processed_regions, CHECKPOINT)

    # 最終保存
    save_checkpoint(acc_counts, processed_regions, CHECKPOINT)
    with open(args.out, "w", encoding="utf8") as f:
        json.dump({"counts": acc_counts, "processed_regions": sorted(list(processed_regions))}, f, ensure_ascii=False, indent=2)
    print("完了。出力:", args.out)


if __name__ == "__main__":
    main()
