import os
import re
import sqlite3
import argparse
from typing import Optional
from datetime import datetime, timezone, timedelta
from email.utils import format_datetime
from xml.dom import minidom
import xml.etree.ElementTree as ET
import zstandard as zstd

DB_PATH = "../shared/wechat"
DB_PATH = "./"
CACHE_DB = "cache.db"
OUTPUT_DIR = "source_xml"

LOCAL_TYPE_FILTER = 21474836529
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
BIZ_DB = os.path.join(DB_PATH, "db_storage/message/biz_message_0.db")
CONTACT_DB = os.path.join(DB_PATH, "db_storage/contact/contact.db")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def decompress_if_needed(data: bytes) -> Optional[str]:
    if data.startswith(ZSTD_MAGIC):
        try:
            return zstd.ZstdDecompressor().decompress(data).decode("utf-8", errors="ignore")
        except Exception:
            return None
    return data.decode("utf-8", errors="ignore")

def partial_unescape(text: str) -> Optional[str]:
    return (
        text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
    )

def add_cdata(parent, tag, text):
    e = ET.SubElement(parent, tag)
    e.text = f"<![CDATA[{text}]]>"

def update_rss(xml_string: str):
    try:
        root = ET.fromstring(xml_string)
        username = root.findtext(".//publisher/username")
        nickname = root.findtext(".//publisher/nickname", default="公众号文章")
        items = root.findall(".//category/item")
        if not username or not items:
            return

        rss = ET.Element("rss", version="2.0", attrib={"xmlns:atom": "http://www.w3.org/2005/Atom"})
        channel = ET.SubElement(rss, "channel")
        add_cdata(channel, "title", nickname)
        ET.SubElement(channel, "link").text = "https://mp.weixin.qq.com/"
        add_cdata(channel, "description", f"{nickname}公众号")
        ET.SubElement(channel, "language").text = "zh-cn"

        image = ET.SubElement(channel, "image")
        ET.SubElement(image, "url").text = f"icon/{username}.jpg"
        ET.SubElement(image, "title").text = username

        for item in items:
            title = re.sub(r"\s+", " ", (item.findtext("title") or "无标题").strip())
            link = item.findtext("url") or ""
            cover = item.findtext("cover") or ""
            summary_raw = item.findtext("summary") or item.findtext("digest") or ""
            summary = re.sub(r"\s+", " ", summary_raw.strip())
            desc = f'<img referrerpolicy="no-referrer" src="{cover}"/><p>{summary}</p>' if cover else f"<p>{summary}</p>"

            try:
                pub_time = int(item.findtext("pub_time"))
                pub_date = datetime.fromtimestamp(pub_time, tz=timezone(timedelta(hours=8)))
            except Exception:
                pub_date = datetime.now(tz=timezone(timedelta(hours=8)))

            rss_item = ET.SubElement(channel, "item")
            add_cdata(rss_item, "title", title)
            add_cdata(rss_item, "description", desc)
            ET.SubElement(rss_item, "link").text = link
            ET.SubElement(rss_item, "pubDate").text = format_datetime(pub_date)

        xml_final = minidom.parseString(ET.tostring(rss, encoding="utf-8")).toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")
        xml_final = partial_unescape(xml_final)
        xml_final = "\n".join([line for line in xml_final.splitlines() if line.strip()])

        rss_path = os.path.join(OUTPUT_DIR, f"{username}.xml")
        if os.path.exists(rss_path):
            with open(rss_path, encoding="utf-8") as f:
                old = f.read()

            old_image_match = re.search(r"<image>\s*<url>(.*?)</url>", old, re.DOTALL)
            if old_image_match:
                old_url = old_image_match.group(1).strip()
                xml_final = re.sub(r"(<image>\s*<url>).*?(</url>)", rf"\1{old_url}\2", xml_final, flags=re.DOTALL)

            old_items = re.findall(r"<item>.*?</item>", old, re.DOTALL)
            new_items = re.findall(r"<item>.*?</item>", xml_final, re.DOTALL)
            merged = new_items + [i for i in old_items if i not in new_items]
            merged = merged[:50]

            xml_final = re.sub(
                r"(<channel>.*?)(<item>.*?</item>\s*)+(.*?</channel>)",
                lambda m: m.group(1) + "\n".join(merged) + "\n" + m.group(3),
                xml_final,
                flags=re.DOTALL
            )

        dom = minidom.parseString(xml_final.encode("utf-8"))
        pretty_xml_str = dom.toprettyxml(indent="  ")

        xml_final_str = partial_unescape(pretty_xml_str)
        xml_final_str = "\n".join([line for line in xml_final_str.splitlines() if line.strip()])

        with open(rss_path, "w", encoding="utf-8") as f:
            f.write(xml_final_str)
            print(f"[+] 更新 {username} - {nickname}, 数量: {len(items)}")

    except ET.ParseError as e:
        print(f"[!] XML解析失败: {e}")

def update_avatar_urls():
    conn = sqlite3.connect(CONTACT_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT username, small_head_url FROM contact")
    avatar_map = dict(cursor.fetchall())
    conn.close()

    for fname in os.listdir(OUTPUT_DIR):
        if not fname.endswith(".xml"):
            continue
        path = os.path.join(OUTPUT_DIR, fname)
        uname = os.path.splitext(fname)[0]
        if uname not in avatar_map:
            continue

        with open(path, encoding="utf-8") as f:
            content = f.read()
        match = re.search(r"<image>\s*<url>(.*?)</url>", content)
        if match and match.group(1).strip() != avatar_map[uname]:
            print(f"[+] 更新 {fname} 的头像 URL")
            content = re.sub(r"(<image>\s*<url>).*?(</url>)", rf"\1{avatar_map[uname]}\2", content, flags=re.DOTALL)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

def update_rss_feeds():
    cache_conn = sqlite3.connect(CACHE_DB)
    cache_cursor = cache_conn.cursor()

    cache_cursor.execute("CREATE TABLE IF NOT EXISTS table_sequence (name TEXT PRIMARY KEY, seq INTEGER)")
    cache_cursor.execute("CREATE TABLE IF NOT EXISTS check_log (id INTEGER PRIMARY KEY, timestamp INTEGER)")
    cache_conn.commit()

    cache_cursor.execute("SELECT MAX(timestamp) FROM check_log")
    last_check = cache_cursor.fetchone()[0] or 0

    biz_conn = sqlite3.connect(BIZ_DB)
    biz_cursor = biz_conn.cursor()
    biz_cursor.execute("SELECT name, seq FROM sqlite_sequence")
    biz_seq = dict(biz_cursor.fetchall())

    cache_cursor.execute("SELECT name, seq FROM table_sequence")
    cache_seq = dict(cache_cursor.fetchall())

    changed = [name for name in biz_seq if biz_seq[name] != cache_seq.get(name)] # 防止KeyError
    for name in changed:
        cache_cursor.execute("REPLACE INTO table_sequence (name, seq) VALUES (?, ?)", (name, biz_seq[name]))

    latest_timestamp = last_check

    #TODO 尚不清楚数据表格是否会被删除或重命名，不清楚deleteinfo的触发条件，先采用遍历全部表格的方式
    # for name in changed:
    all_name = [name for name in biz_seq]
    for name in all_name:
        try:
            if last_check:
                sql = f"""SELECT message_content, create_time 
                          FROM "{name}" 
                          WHERE local_type = ? AND create_time > ?"""
                params = (LOCAL_TYPE_FILTER, last_check)
            else:
                sql = f"""SELECT message_content, create_time 
                          FROM "{name}" 
                          WHERE local_type = ?"""
                params = (LOCAL_TYPE_FILTER,)

            biz_cursor.execute(sql, params)
            for msg, create_time in biz_cursor.fetchall():
                if create_time > latest_timestamp:
                    latest_timestamp = create_time
                xml = decompress_if_needed(msg)
                if xml and "<msg>" in xml:
                    update_rss(xml)
        except Exception as e:
            print(f"[!] 处理 {name} 时出错: {e}")

    if latest_timestamp > last_check:
        cache_cursor.execute("INSERT INTO check_log (timestamp) VALUES (?)", (latest_timestamp,))

    cache_conn.commit()
    biz_conn.close()
    cache_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--func", choices=["rss", "avatar", "all"], default="all", help="执行模式")
    args = parser.parse_args()

    if args.func in ("rss", "all"):
        update_rss_feeds()
    if args.func in ("avatar", "all"):
        update_avatar_urls()
