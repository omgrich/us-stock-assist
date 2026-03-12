"""
notion_fix_properties.py - 检查并修复 Notion 数据库属性

对 config.yaml 中每个已配置的数据库：
  1. 获取当前属性列表
  2. 对比目标 schema（与 notion_setup.py 保持一致）
  3. 添加缺失的属性（Notion API 不允许删除Title，只会新增缺失列）

用法：
  python notion_fix_properties.py           # 检查 + 修复所有数据库
  python notion_fix_properties.py --check   # 只检查，不修改
  python notion_fix_properties.py --db weekly_report  # 只修复指定数据库
"""

import argparse
import json
import yaml
import httpx
from notion_client import Client
from notion_client.errors import APIResponseError

CONFIG_PATH = "config/config.yaml"
NOTION_VERSION = "2022-06-28"


def raw_patch_database(token: str, db_id: str, properties: dict):
    """Directly PATCH database schema via httpx (notion-client 3.x silently drops properties)"""
    resp = httpx.patch(
        f"https://api.notion.com/v1/databases/{db_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        },
        content=json.dumps({"properties": properties}, ensure_ascii=False).encode(),
        timeout=30,
    )
    if not resp.is_success:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json()

# 与 notion_setup.py 保持一致的目标 schema（不含 title 类型，title 单独处理）
TARGET_SCHEMAS = {
    "weekly_report": {
        "报告标题": {"title": {}},
        "日期":     {"date": {}},
        "编辑寄语": {"rich_text": {}},
        "做多标的": {"rich_text": {}},
        "做多入场": {"rich_text": {}},
        "做多止损": {"rich_text": {}},
        "做多目标": {"rich_text": {}},
        "做空标的": {"rich_text": {}},
    },
    "insider_buying": {
        "标的":          {"title": {}},
        "分析日期":      {"date": {}},
        "内部人职位":    {"rich_text": {}},
        "买入金额(USD)": {"number": {"format": "dollar"}},
        "买入价格":      {"number": {"format": "dollar"}},
        "当前价格":      {"number": {"format": "dollar"}},
        "价格变化%":     {"number": {"format": "percent"}},
        "为何重要":      {"rich_text": {}},
        "风险因素":      {"rich_text": {}},
        "来源链接":      {"url": {}},
    },
    "short_squeeze": {
        "标的":               {"title": {}},
        "分析日期":           {"date": {}},
        "空头比例%":          {"number": {"format": "percent"}},
        "平仓天数":           {"number": {"format": "number"}},
        "借入利率%":          {"number": {"format": "percent"}},
        "催化剂":             {"rich_text": {}},
        "催化剂时间":         {"rich_text": {}},
        "入场策略":           {"rich_text": {}},
        "挤压失败风险(1-10)": {"number": {"format": "number"}},
        "失败原因":           {"rich_text": {}},
        "来源":               {"url": {}},
    },
    "ma_radar": {
        "标的":          {"title": {}},
        "分析日期":      {"date": {}},
        "行业":          {"select": {"options": [
            {"name": "科技", "color": "blue"},
            {"name": "医疗", "color": "green"},
            {"name": "金融", "color": "yellow"},
            {"name": "消费", "color": "orange"},
            {"name": "能源", "color": "red"},
            {"name": "工业", "color": "gray"},
            {"name": "其他", "color": "default"},
        ]}},
        "当前价格":      {"number": {"format": "dollar"}},
        "预计收购溢价%": {"number": {"format": "percent"}},
        "隐含收购价":    {"number": {"format": "dollar"}},
        "潜在收购方":    {"rich_text": {}},
        "催化剂证据":    {"rich_text": {}},
        "监管风险":      {"select": {"options": [
            {"name": "低", "color": "green"},
            {"name": "中", "color": "yellow"},
            {"name": "高", "color": "red"},
        ]}},
        "交易概率":      {"select": {"options": [
            {"name": "低", "color": "red"},
            {"name": "中", "color": "yellow"},
            {"name": "高", "color": "green"},
        ]}},
        "来源":          {"url": {}},
    },
    "sentiment_arb": {
        "标的":         {"title": {}},
        "分析日期":     {"date": {}},
        "负面情绪原因": {"rich_text": {}},
        "基本面矛盾点": {"rich_text": {}},
        "基本面优势":   {"rich_text": {}},
        "重估催化剂":   {"rich_text": {}},
        "持有周期":     {"select": {"options": [
            {"name": "1个月内",  "color": "red"},
            {"name": "1-3个月",  "color": "yellow"},
            {"name": "3-6个月",  "color": "blue"},
            {"name": "6-12个月", "color": "green"},
            {"name": "12个月+",  "color": "purple"},
        ]}},
        "来源":         {"url": {}},
    },
    "institutional": {
        "标的":         {"title": {}},
        "报告季度":     {"rich_text": {}},
        "分析日期":     {"date": {}},
        "买入基金":     {"rich_text": {}},
        "总仓位规模(M)":{"number": {"format": "number"}},
        "行业":         {"select": {"options": [
            {"name": "科技", "color": "blue"},
            {"name": "医疗", "color": "green"},
            {"name": "金融", "color": "yellow"},
            {"name": "消费", "color": "orange"},
            {"name": "能源", "color": "red"},
            {"name": "工业", "color": "gray"},
            {"name": "其他", "color": "default"},
        ]}},
        "推测逻辑":     {"rich_text": {}},
        "信号类型":     {"select": {"options": [
            {"name": "共识买入", "color": "green"},
            {"name": "新建仓",   "color": "blue"},
            {"name": "大幅减仓", "color": "red"},
            {"name": "清仓",     "color": "pink"},
        ]}},
    },
    "macro_analysis": {
        "分析标题":       {"title": {}},
        "分析日期":       {"date": {}},
        "收益率曲线状态": {"select": {"options": [
            {"name": "正常", "color": "green"},
            {"name": "平坦", "color": "yellow"},
            {"name": "倒挂", "color": "red"},
        ]}},
        "10Y-2Y利差":  {"number": {"format": "number"}},
        "偏好行业":     {"rich_text": {}},
        "规避行业":     {"rich_text": {}},
        "3个月宏观展望": {"rich_text": {}},
        "关键风险":    {"rich_text": {}},
        "历史类比期":  {"rich_text": {}},
        "来源":        {"url": {}},
    },
    "dividend_danger": {
        "标的":         {"title": {}},
        "分析日期":     {"date": {}},
        "当前股息率%":  {"number": {"format": "percent"}},
        "派息比率%":    {"number": {"format": "percent"}},
        "自由现金流覆盖": {"select": {"options": [
            {"name": "充裕覆盖", "color": "green"},
            {"name": "勉强覆盖", "color": "yellow"},
            {"name": "无法覆盖", "color": "red"},
        ]}},
        "净债务/EBITDA": {"number": {"format": "number"}},
        "降息概率":     {"select": {"options": [
            {"name": "低", "color": "green"},
            {"name": "中", "color": "yellow"},
            {"name": "高", "color": "red"},
        ]}},
        "降息原因":     {"rich_text": {}},
        "更安全替代品": {"rich_text": {}},
        "来源":         {"url": {}},
    },
}


def fix_database(token: str, db_key: str, db_id: str, check_only: bool):
    schema = TARGET_SCHEMAS.get(db_key)
    if not schema:
        print(f"  [{db_key}] 无目标 schema 定义，跳过")
        return

    print(f"\n{'─'*55}")
    print(f"  数据库: {db_key}  (ID: {db_id})")

    title_name = next((k for k, v in schema.items() if "title" in v), None)
    non_title  = {k: v for k, v in schema.items() if "title" not in v}

    if check_only:
        print(f"  目标 title 属性: {title_name}")
        print(f"  目标普通属性 ({len(non_title)}): {', '.join(sorted(non_title.keys()))}")
        return

    # Step 1: 重命名 title 列（默认列名为 "Name"）
    if title_name:
        for current_name in ("Name", title_name):
            try:
                raw_patch_database(token, db_id, {current_name: {"name": title_name}})
                print(f"  ✅ Title 列: '{current_name}' → '{title_name}'")
                break
            except RuntimeError as e:
                if "not a property that exists" in str(e):
                    continue
                print(f"  ⚠️  Title 重命名失败 ('{current_name}'): {e}")
                break

    # Step 2: 添加/更新所有非 title 属性
    if non_title:
        try:
            result = raw_patch_database(token, db_id, non_title)
            actual = [k for k in result.get("properties", {}) if k != title_name]
            print(f"  ✅ 已添加/更新 {len(non_title)} 个属性: {', '.join(non_title.keys())}")
            print(f"  📋 数据库现有属性: {list(result.get('properties', {}).keys())}")
        except RuntimeError as e:
            print(f"  ❌ 添加属性失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="检查并修复 Notion 数据库属性")
    parser.add_argument("--check", action="store_true", help="只检查，不修改")
    parser.add_argument("--db", type=str, help="只处理指定数据库 key（如 weekly_report）")
    args = parser.parse_args()

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    token     = config["notion"]["token"]
    databases = config["notion"].get("databases", {})

    mode = "检查模式（只读）" if args.check else "修复模式（会修改 Notion）"
    print(f"\n🔍 Notion 数据库属性检查工具  [{mode}]\n")

    for key, db_id in databases.items():
        if db_id == "YOUR_DATABASE_ID":
            print(f"\n  [{key}] 未配置，跳过")
            continue
        if args.db and args.db != key:
            continue
        fix_database(token, key, db_id, check_only=args.check)

    print(f"\n{'─'*55}")
    print("完成。\n")


if __name__ == "__main__":
    main()
