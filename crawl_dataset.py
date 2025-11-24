"""
Merge Multiple TikTok Datasets
Gộp tất cả checkpoint và boost data thành 1 dataset hoàn chỉnh
"""

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def load_all_json_files(directory='tiktok_dataset'):
    """Load tất cả file JSON trong thư mục"""
    all_videos = []
    data_dir = Path(directory)
    
    if not data_dir.exists():
        print(f"✗ Thư mục {directory} không tồn tại!")
        return []
    
    json_files = list(data_dir.glob('*.json'))
    
    if not json_files:
        print(f"✗ Không tìm thấy file JSON nào trong {directory}/")
        return []
    
    print(f"📁 Tìm thấy {len(json_files)} files JSON")
    print("="*60)
    
    for filepath in json_files:
        # Skip statistics file
        if 'statistics' in filepath.name:
            continue
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both list and dict formats
                if isinstance(data, list):
                    videos = data
                elif isinstance(data, dict) and 'videos' in data:
                    videos = data['videos']
                else:
                    continue
                
                all_videos.extend(videos)
                print(f"  ✓ {filepath.name:40s} → {len(videos):>5,} videos")
                
        except Exception as e:
            print(f"  ✗ {filepath.name:40s} → Error: {str(e)[:30]}")
    
    return all_videos

def deduplicate_videos(videos):
    """Remove duplicates by video_id"""
    print(f"\n🔄 Deduplicating...")
    print(f"   Before: {len(videos):,} videos")
    
    # Use dict to keep last occurrence
    unique = {}
    for video in videos:
        video_id = video.get('video_id', '')
        if video_id:
            unique[video_id] = video
    
    unique_videos = list(unique.values())
    print(f"   After:  {len(unique_videos):,} videos")
    print(f"   ✓ Removed {len(videos) - len(unique_videos):,} duplicates")
    
    return unique_videos

def analyze_dataset(videos):
    """Phân tích dataset"""
    print(f"\n{'='*80}")
    print("📊 DATASET ANALYSIS")
    print(f"{'='*80}")
    
    # Basic stats
    total = len(videos)
    print(f"\n📈 OVERVIEW:")
    print(f"   Total videos: {total:,}")
    
    # Date range
    dates = [v['create_date'] for v in videos if 'create_date' in v]
    if dates:
        print(f"   Date range: {min(dates)} → {max(dates)}")
    
    # By year
    by_year = defaultdict(int)
    for v in videos:
        if 'year' in v:
            by_year[v['year']] += 1
    
    print(f"\n📅 DISTRIBUTION BY YEAR:")
    for year in sorted(by_year.keys()):
        count = by_year[year]
        percentage = (count / total) * 100
        bar = '█' * int(percentage / 2)
        print(f"   {year}: {count:>6,} ({percentage:5.1f}%) {bar}")
    
    # Top hashtags
    hashtag_count = defaultdict(int)
    for v in videos:
        for tag in v.get('hashtags', []):
            hashtag_count[tag] += 1
    
    top_tags = sorted(hashtag_count.items(), key=lambda x: x[1], reverse=True)[:15]
    print(f"\n🏷️  TOP 15 HASHTAGS:")
    for tag, count in top_tags:
        print(f"   #{tag:20s} → {count:>5,} videos")
    
    # Engagement stats
    total_views = sum(v.get('views', 0) for v in videos)
    total_likes = sum(v.get('likes', 0) for v in videos)
    
    print(f"\n💰 ENGAGEMENT:")
    print(f"   Total views: {total_views:>15,}")
    print(f"   Total likes: {total_likes:>15,}")
    print(f"   Avg views/video: {total_views//total if total > 0 else 0:>10,}")
    print(f"   Avg likes/video: {total_likes//total if total > 0 else 0:>10,}")
    
    # Unique counts
    unique_authors = len(set(v.get('author_username', '') for v in videos))
    unique_hashtags = len(hashtag_count)
    
    print(f"\n🎯 DIVERSITY:")
    print(f"   Unique authors: {unique_authors:,}")
    print(f"   Unique hashtags: {unique_hashtags:,}")
    
    return {
        'total_videos': total,
        'date_range': {'earliest': min(dates) if dates else None, 'latest': max(dates) if dates else None},
        'by_year': dict(by_year),
        'top_hashtags': top_tags[:15],
        'unique_authors': unique_authors,
        'unique_hashtags': unique_hashtags,
        'total_views': total_views,
        'total_likes': total_likes
    }

def prepare_for_mining(videos):
    """Chuẩn bị transaction database"""
    print(f"\n🔄 Preparing transaction database...")
    
    transactions = []
    
    for video in videos:
        items = set()
        
        # Hashtags (top 5)
        items.update([f"tag_{tag}" for tag in video.get('hashtags', [])[:5]])
        
        # Temporal
        if 'year' in video:
            items.add(f"year_{video['year']}")
        if 'quarter' in video:
            items.add(f"q{video['quarter']}_{video['year']}")
        
        # Music
        music_title = video.get('music_title', '')
        if music_title and music_title != 'original sound':
            items.add(f"music_{music_title[:30]}")
        
        # Author verified
        if video.get('author_verified'):
            items.add("verified_author")
        
        # Engagement level
        views = video.get('views', 0)
        likes = video.get('likes', 0)
        if views > 0:
            engagement_rate = (likes / views) * 100
            if engagement_rate > 5:
                items.add("high_engagement")
            elif engagement_rate > 2:
                items.add("medium_engagement")
            else:
                items.add("low_engagement")
        
        # Duration categories
        duration = video.get('duration', 0)
        if duration < 15:
            items.add("short_video")
        elif duration < 60:
            items.add("medium_video")
        else:
            items.add("long_video")
        
        # Popularity
        if views > 1000000:
            items.add("viral")
        elif views > 100000:
            items.add("popular")
        elif views > 10000:
            items.add("trending")
        else:
            items.add("normal")
        
        if items:
            transactions.append({
                'transaction_id': video.get('video_id', ''),
                'items': list(items),
                'year': video.get('year', 0),
                'timestamp': video.get('create_time', ''),
                'video_url': video.get('video_url', '')
            })
    
    print(f"   ✓ Created {len(transactions):,} transactions")
    return transactions

def export_merged_data(videos, transactions, output_dir='tiktok_final'):
    """Export merged dataset"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n{'='*80}")
    print("💾 EXPORTING MERGED DATA")
    print(f"{'='*80}")
    
    # 1. Raw JSON
    raw_file = output_path / 'merged_raw_data.json'
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)
    print(f"✓ {raw_file}")
    
    # 2. CSV
    df = pd.DataFrame(videos)
    csv_file = output_path / 'merged_videos.csv'
    df.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"✓ {csv_file}")
    
    # 3. Transactions TXT (Apriori format)
    trans_txt = output_path / 'transactions.txt'
    with open(trans_txt, 'w', encoding='utf-8') as f:
        for trans in transactions:
            f.write(','.join(trans['items']) + '\n')
    print(f"✓ {trans_txt}")
    
    # 4. Transactions JSON
    trans_json = output_path / 'transactions.json'
    with open(trans_json, 'w', encoding='utf-8') as f:
        json.dump(transactions, f, ensure_ascii=False, indent=2)
    print(f"✓ {trans_json}")
    
    # 5. Statistics
    stats = analyze_dataset(videos)
    stats_file = output_path / 'final_statistics.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"✓ {stats_file}")
    
    print(f"\n✅ All data exported to: {output_path}/")
    
    return output_path

def main():
    """Main merge process"""
    print("="*80)
    print("🔗 TikTok Dataset Merger")
    print("="*80)
    
    # Input directory
    input_dir = input("\nNhập thư mục chứa data [tiktok_dataset]: ").strip() or 'tiktok_dataset'
    
    # Load all files
    print(f"\n📂 Loading from: {input_dir}/")
    all_videos = load_all_json_files(input_dir)
    
    if not all_videos:
        print("\n✗ Không có dữ liệu để merge!")
        return
    
    print(f"\n✓ Loaded {len(all_videos):,} videos total")
    
    # Deduplicate
    unique_videos = deduplicate_videos(all_videos)
    
    # Analyze
    stats = analyze_dataset(unique_videos)
    
    # Prepare transactions
    transactions = prepare_for_mining(unique_videos)
    
    # Export
    output_dir = input("\nNhập thư mục output [tiktok_final]: ").strip() or 'tiktok_final'
    output_path = export_merged_data(unique_videos, transactions, output_dir)
    
    # Final summary
    print(f"\n{'='*80}")
    print("✅ MERGE COMPLETED SUCCESSFULLY!")
    print(f"{'='*80}")
    print(f"📊 Final dataset: {len(unique_videos):,} videos")
    print(f"📊 Transactions: {len(transactions):,}")
    print(f"📅 Years covered: {stats['by_year'].keys()}")
    print(f"📁 Output: {output_path}/")
    
    print(f"\n🎓 READY FOR ANALYSIS:")
    print(f"   • Raw data: merged_raw_data.json")
    print(f"   • CSV: merged_videos.csv")
    print(f"   • Transactions: transactions.txt & transactions.json")
    print(f"   • Stats: final_statistics.json")
    
    print(f"\n💡 NEXT STEPS:")
    print(f"   1. Áp dụng Apriori/FP-Growth trên transactions.txt")
    print(f"   2. Khai thác Maximal Frequent Itemsets")
    print(f"   3. Phân tích temporal patterns")
    print(f"   4. Làm giàu dữ liệu cho unsupervised learning")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Cancelled!")
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()