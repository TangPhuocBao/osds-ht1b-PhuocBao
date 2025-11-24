from TikTokApi import TikTokApi
from datetime import datetime, timedelta, timezone
import json
import asyncio
import random
import os

class TikTokTrendingScraper:
    def __init__(self):
        self.api = None

    async def initialize(self):
        """Khởi tạo TikTok API với auto-install browser"""
        try:
            print("🔧 Kiểm tra browser Playwright...")

            # Auto install Chromium nếu chưa có
            import subprocess
            subprocess.run(["python", "-m", "playwright", "install", "chromium"], stdout=subprocess.DEVNULL)

            self.api = TikTokApi()

            await self.api.create_sessions(
                num_sessions=1,
                sleep_after=3,
                headless=True  # chạy headless cho nhẹ – nhanh – ít crash
            )

            print("✓ API initialized")
            return True

        except Exception as e:
            print("✗ Lỗi init:", e)
            return False

    async def get_trending_videos(self, count=30):
        """Lấy video trending"""
        videos = []
        print(f"Đang lấy {count} video trending...")

        try:
            async for video in self.api.trending(count=count):
                info = await self.parse_video(video)
                if info:
                    videos.append(info)
                    print("  ✓", info['description'][:40])

                await asyncio.sleep(random.uniform(1, 3))

        except Exception as e:
            print("✗ Lỗi trending:", e)

        return videos

    async def parse_video(self, video):
        """Parse video an toàn"""
        try:
            video_dict = video.as_dict()

            stats = video_dict.get("stats", {})
            author = video_dict.get("author", {})
            music = video_dict.get("music", {})

            create_time = datetime.fromtimestamp(video_dict.get("createTime", 0), tz=timezone.utc)

            return {
                "video_id": video_dict.get("id"),
                "description": video_dict.get("desc", ""),
                "author": author.get("uniqueId"),
                "author_nickname": author.get("nickname", ""),
                "author_verified": author.get("verified", False),
                "likes": stats.get("diggCount", 0),
                "comments": stats.get("commentCount", 0),
                "shares": stats.get("shareCount", 0),
                "views": stats.get("playCount", 0),
                "music": music.get("title", ""),
                "music_author": music.get("authorName", ""),
                "hashtags": [ch.get("title") for ch in video_dict.get("challenges", [])],
                "duration": video_dict.get("video", {}).get("duration", 0),
                "create_time": create_time.strftime("%Y-%m-%d %H:%M:%S"),
                "video_url": f"https://www.tiktok.com/@{author.get('uniqueId')}/video/{video_dict.get('id')}",
            }

        except Exception:
            return None

    def filter_videos_by_time(self, videos, hours=24):
        """Lọc video theo thời gian đăng"""
        if not videos:
            return []

        now = datetime.now(timezone.utc)
        time_limit = now - timedelta(hours=hours)

        result = []
        for v in videos:
            try:
                t = datetime.strptime(v["create_time"], "%Y-%m-%d %H:%M:%S")
                t = t.replace(tzinfo=timezone.utc)
                if t >= time_limit:
                    result.append(v)
            except:
                pass

        return result

    async def get_trending_videos_by_time(self, total_fetch_count=80, filter_hours=24):
        """Lấy trending rồi lọc thời gian"""
        all_videos = await self.get_trending_videos(total_fetch_count)
        return self.filter_videos_by_time(all_videos, filter_hours)

    async def close(self):
        try:
            if self.api:
                await self.api.close_sessions()
        except:
            pass


def save_to_json(videos, filename="tiktok_data.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)
    print("✓ Đã lưu", filename)


async def main():
    scraper = TikTokTrendingScraper()
    if not await scraper.initialize():
        return

    videos = await scraper.get_trending_videos_by_time(
        total_fetch_count=80,
        filter_hours=24
    )

    save_to_json(videos)
    await scraper.close()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
