from TikTokApi import TikTokApi
from datetime import datetime
import json
import asyncio
import os

class TikTokTrendingScraper:
    def __init__(self):
        self.api = None
        
    async def initialize(self):
        """Khởi tạo TikTok API"""
        try:
            # Khởi tạo API
            self.api = TikTokApi()
            await self.api.create_sessions(
                num_sessions=1,
                sleep_after=3,
                headless=True
            )
            print("✓ Đã khởi tạo TikTok API thành công!")
            return True
        except Exception as e:
            error_msg = str(e)
            print(f"✗ Lỗi khi khởi tạo API: {error_msg}")
            
            # Kiểm tra lỗi cụ thể về browser
            if "Executable doesn't exist" in error_msg or "playwright install" in error_msg:
                print("\n⚠️  Browser chưa được cài đặt!")
                print("\n🔧 KHẮC PHỤC:")
                print("   Chạy lệnh sau trong terminal/cmd:")
                print("   → python -m playwright install chromium")
                print("\n   Hoặc cài đặt tất cả browsers:")
                print("   → python -m playwright install")
            
            return False
    
    async def get_trending_videos(self, count=30):
        """
        Lấy danh sách video trending từ TikTok
        
        Args:
            count: Số lượng video muốn lấy (mặc định 30)
        """
        videos = []
        
        try:
            print(f"Đang lấy {count} video trending...")
            
            # Lấy video trending
            async for video in self.api.trending.videos(count=count):
                video_info = await self.parse_video(video)
                if video_info:
                    videos.append(video_info)
                    print(f"  ✓ Đã lấy: {video_info['description'][:50]}...")
                
        except Exception as e:
            print(f"Lỗi khi lấy dữ liệu: {str(e)}")
        
        return videos
    
    async def parse_video(self, video):
        """Parse dữ liệu video"""
        try:
            video_dict = video.as_dict
            stats = video_dict.get('stats', {})
            author = video_dict.get('author', {})
            music = video_dict.get('music', {})
            
            video_info = {
                'video_id': video_dict.get('id', ''),
                'description': video_dict.get('desc', ''),
                'author': author.get('uniqueId', ''),
                'author_nickname': author.get('nickname', ''),
                'author_verified': author.get('verified', False),
                'music': music.get('title', ''),
                'music_author': music.get('authorName', ''),
                'likes': stats.get('diggCount', 0),
                'comments': stats.get('commentCount', 0),
                'shares': stats.get('shareCount', 0),
                'views': stats.get('playCount', 0),
                'video_url': f"https://www.tiktok.com/@{author.get('uniqueId', '')}/video/{video_dict.get('id', '')}",
                'hashtags': [tag['title'] for tag in video_dict.get('challenges', [])],
                'create_time': datetime.fromtimestamp(video_dict.get('createTime', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'duration': video_dict.get('video', {}).get('duration', 0),
            }
            return video_info
            
        except Exception as e:
            print(f"Lỗi khi parse video: {str(e)}")
            return None
    
    async def search_videos(self, keyword, count=20):
        """
        Tìm kiếm video theo từ khóa
        
        Args:
            keyword: Từ khóa tìm kiếm
            count: Số lượng video
        """
        videos = []
        
        try:
            print(f"Đang tìm kiếm: '{keyword}'...")
            
            async for video in self.api.search.videos(keyword, count=count):
                video_info = await self.parse_video(video)
                if video_info:
                    videos.append(video_info)
                    
        except Exception as e:
            print(f"Lỗi khi tìm kiếm: {str(e)}")
        
        return videos
    
    async def get_hashtag_videos(self, hashtag, count=20):
        """
        Lấy video từ hashtag
        
        Args:
            hashtag: Tên hashtag (không cần #)
            count: Số lượng video
        """
        videos = []
        
        try:
            print(f"Đang lấy video từ #{hashtag}...")
            
            tag = self.api.hashtag(name=hashtag)
            async for video in tag.videos(count=count):
                video_info = await self.parse_video(video)
                if video_info:
                    videos.append(video_info)
                    
        except Exception as e:
            print(f"Lỗi khi lấy video hashtag: {str(e)}")
        
        return videos
    
    async def get_user_videos(self, username, count=20):
        """
        Lấy video từ user
        
        Args:
            username: Tên user (không cần @)
            count: Số lượng video
        """
        videos = []
        
        try:
            print(f"Đang lấy video từ @{username}...")
            
            user = self.api.user(username=username)
            async for video in user.videos(count=count):
                video_info = await self.parse_video(video)
                if video_info:
                    videos.append(video_info)
                    
        except Exception as e:
            print(f"Lỗi khi lấy video user: {str(e)}")
        
        return videos
    
    async def close(self):
        """Đóng API session"""
        try:
            if self.api:
                await self.api.close_sessions()
                # Đợi một chút để cleanup hoàn tất
                await asyncio.sleep(0.5)
        except Exception as e:
            pass  # Bỏ qua lỗi cleanup
    

def save_to_json(videos, filename='tiktok_trending.json'):
    """Lưu dữ liệu vào file JSON"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(videos, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Đã lưu {len(videos)} video vào {filename}")
    except Exception as e:
        print(f"✗ Lỗi khi lưu file: {str(e)}")

def display_videos(videos):
    """Hiển thị thông tin video"""
    print(f"\n{'='*80}")
    print(f"TỔNG SỐ VIDEO: {len(videos)}")
    print(f"{'='*80}\n")
    
    for i, video in enumerate(videos, 1):
        verified = "✓" if video.get('author_verified') else ""
        print(f"[{i}] {video['description'][:60]}...")
        print(f"    👤 Tác giả: @{video['author']} ({video['author_nickname']}) {verified}")
        print(f"    ❤️  Likes: {video['likes']:,} | 💬 Comments: {video['comments']:,}")
        print(f"    👁️  Views: {video['views']:,} | 🔄 Shares: {video['shares']:,}")
        print(f"    🎵 Nhạc: {video['music']} - {video.get('music_author', '')}")
        if video['hashtags']:
            print(f"    #️⃣  Tags: {', '.join(video['hashtags'][:3])}")
        print(f"    ⏱️  Thời lượng: {video.get('duration', 0)}s | 🕒 Đăng: {video['create_time']}")
        print(f"    🔗 URL: {video['video_url']}")
        print()

def display_stats(videos):
    """Hiển thị thống kê"""
    if not videos:
        return
    
    total_views = sum(v['views'] for v in videos)
    total_likes = sum(v['likes'] for v in videos)
    total_comments = sum(v['comments'] for v in videos)
    total_shares = sum(v['shares'] for v in videos)
    
    print(f"\n📊 THỐNG KÊ:")
    print(f"   • Tổng lượt xem: {total_views:,}")
    print(f"   • Tổng lượt thích: {total_likes:,}")
    print(f"   • Tổng bình luận: {total_comments:,}")
    print(f"   • Tổng chia sẻ: {total_shares:,}")
    print(f"   • Trung bình views/video: {total_views//len(videos):,}")
    print(f"   • Trung bình likes/video: {total_likes//len(videos):,}")

async def main():
    """Hàm chính"""
    print("=" * 80)
    print("🎵 TikTok Video Scraper với TikTokApi 🎵")
    print("=" * 80)
    
    scraper = TikTokTrendingScraper()
    
    # Khởi tạo API
    if not await scraper.initialize():
        print("\n⚠️  Không thể khởi tạo TikTok API")
        print("\n💡 SAU KHI CÀI ĐẶT XONG:")
        print("   Chạy lại script này: python tiktok_scraper.py")
        
        # Cleanup trước khi thoát
        await scraper.close()
        return
    
    try:
        # Menu lựa chọn
        print("\n📋 CHỌN CHỨC NĂNG:")
        print("1. Lấy video trending")
        print("2. Tìm kiếm video theo từ khóa")
        print("3. Lấy video từ hashtag")
        print("4. Lấy video từ user")
        
        choice = input("\nNhập lựa chọn (1-4) [mặc định: 1]: ").strip() or "1"
        
        videos = []
        
        if choice == "1":
            count = int(input("Số lượng video [mặc định: 20]: ").strip() or "20")
            videos = await scraper.get_trending_videos(count=count)
            
        elif choice == "2":
            keyword = input("Nhập từ khóa tìm kiếm: ").strip()
            count = int(input("Số lượng video [mặc định: 20]: ").strip() or "20")
            if keyword:
                videos = await scraper.search_videos(keyword, count=count)
                
        elif choice == "3":
            hashtag = input("Nhập hashtag (không cần #): ").strip()
            count = int(input("Số lượng video [mặc định: 20]: ").strip() or "20")
            if hashtag:
                videos = await scraper.get_hashtag_videos(hashtag, count=count)
                
        elif choice == "4":
            username = input("Nhập username (không cần @): ").strip()
            count = int(input("Số lượng video [mặc định: 20]: ").strip() or "20")
            if username:
                videos = await scraper.get_user_videos(username, count=count)
        
        # Hiển thị kết quả
        if videos:
            display_videos(videos)
            display_stats(videos)
            
            # Lưu file
            save = input("\nLưu vào file JSON? (y/n) [mặc định: y]: ").strip().lower()
            if save != 'n':
                filename = input("Tên file [mặc định: tiktok_data.json]: ").strip() or "tiktok_data.json"
                save_to_json(videos, filename)
        else:
            print("\n⚠️  Không lấy được video nào.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Đã dừng chương trình")
    except Exception as e:
        print(f"\n✗ Lỗi: {str(e)}")
    finally:
        # Đóng API và cleanup
        print("\n🔄 Đang dọn dẹp...")
        await scraper.close()
        print("✓ Hoàn tất!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Tạm biệt!")
    except Exception as e:
        print(f"\n✗ Lỗi nghiêm trọng: {str(e)}")