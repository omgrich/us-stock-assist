"""
claude_runner.py - AWS Bedrock + Tool Use 版本
替换原来的 Claude Code CLI 方案

流程：
  1. 把提示词 + 4个 Tool Schema 发给 Bedrock
  2. LLM 自主决定调用哪个工具、传什么参数
  3. 程序执行工具，把结果返回给 LLM
  4. 循环直到 LLM 输出最终 JSON 结果
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from tools import TOOL_SCHEMAS, execute_tool

logger = logging.getLogger(__name__)


class ClaudeRunner:
    def __init__(self, config: dict):
        self.model_id = config.get("model_id", "anthropic.claude-sonnet-4-5-20251101")
        self.region = config.get("region", "us-east-1")
        self.max_tokens = config.get("max_tokens", 4000)
        self.max_tool_rounds = config.get("max_tool_rounds", 10)  # 防止死循环
        self.results_dir = Path(config.get("results_dir", "./results"))
        self.results_dir.mkdir(parents=True, exist_ok=True)

        # Tool 执行需要的 API Keys
        self.api_keys = {
            "tavily": config.get("tavily_api_key", ""),
            "fred": config.get("fred_api_key", ""),
        }

        # 初始化 Bedrock 客户端
        boto3_kwargs = {"region_name": self.region}
        aws_key = config.get("aws_access_key_id", "")
        aws_secret = config.get("aws_secret_access_key", "")
        if aws_key and aws_secret and aws_key != "YOUR_AWS_ACCESS_KEY_ID":
            boto3_kwargs["aws_access_key_id"] = aws_key
            boto3_kwargs["aws_secret_access_key"] = aws_secret
        self.bedrock = boto3.client("bedrock-runtime", **boto3_kwargs)

    def build_prompt(self, template: str, variables: dict = None) -> str:
        """将模板变量注入提示词"""
        variables = variables or {}
        variables["DATE"] = datetime.now().strftime("%Y-%m-%d")
        variables["TIME"] = datetime.now().strftime("%H:%M")
        prompt = template
        for key, value in variables.items():
            prompt = prompt.replace(f"{{{key}}}", str(value))
        return prompt

    def run(self, task_name: str, prompt: str) -> dict:
        """
        主执行方法：Bedrock Tool Use 循环
        LLM 自主调用工具直到完成任务
        """
        logger.info(f"[{task_name}] 开始执行 @ {datetime.now().strftime('%H:%M:%S')}")

        messages = [{"role": "user", "content": [{"text": prompt}]}]
        tool_call_count = 0

        try:
            for round_num in range(self.max_tool_rounds + 1):

                # 调用 Bedrock Converse API
                response = self._invoke_bedrock(messages)
                stop_reason = response["stopReason"]
                content = response["output"]["message"]["content"]

                logger.info(f"[{task_name}] Round {round_num} - stop_reason: {stop_reason}")

                # ── 情况1：LLM 完成，输出最终结果 ──
                if stop_reason == "end_turn":
                    final_text = self._extract_text(content)
                    parsed = self._extract_json(final_text)

                    if not parsed:
                        logger.warning(f"[{task_name}] 无法解析 JSON，保存原始输出")
                        parsed = {"raw_output": final_text, "parse_error": True}

                    parsed["_meta"] = {
                        "task_name": task_name,
                        "executed_at": datetime.now().isoformat(),
                        "tool_calls": tool_call_count,
                        "rounds": round_num,
                        "success": "parse_error" not in parsed
                    }

                    self._save_result(task_name, parsed)
                    logger.info(f"[{task_name}] 完成，共调用工具 {tool_call_count} 次")
                    return parsed

                # ── 情况2：LLM 要调用工具 ──
                elif stop_reason == "tool_use":
                    messages.append({
                        "role": "assistant",
                        "content": content
                    })

                    tool_results = []
                    for block in content:
                        logger.debug(f"[{task_name}] content block keys: {list(block.keys())}")
                        tool_use = block.get("toolUse")
                        if tool_use:
                            tool_call_count += 1
                            output = execute_tool(
                                tool_use["name"],
                                tool_use["input"],
                                self.api_keys
                            )
                            tool_results.append({
                                "toolResult": {
                                    "toolUseId": tool_use["toolUseId"],
                                    "content": [{"text": output or "(no output)"}]
                                }
                            })

                    if not tool_results:
                        logger.error(f"[{task_name}] stop_reason=tool_use 但未找到任何 toolUse block，content={content}")
                        break

                    messages.append({"role": "user", "content": tool_results})

                else:
                    logger.warning(f"[{task_name}] 意外的 stop_reason: {stop_reason}")
                    break

            logger.error(f"[{task_name}] 超过最大工具调用轮次 ({self.max_tool_rounds})")
            return self._error_result(task_name, "EXCEEDED_MAX_ROUNDS")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"[{task_name}] Bedrock 错误 {error_code}: {e}")
            return self._error_result(task_name, f"BEDROCK_ERROR: {error_code}")

        except Exception as e:
            logger.exception(f"[{task_name}] 未预期错误: {e}")
            return self._error_result(task_name, str(e))

    def _invoke_bedrock(self, messages: list) -> dict:
        """调用 Bedrock Converse API"""
        return self.bedrock.converse(
            modelId=self.model_id,
            messages=messages,
            inferenceConfig={
                "maxTokens": self.max_tokens,
                "temperature": 0.1,
            },
            toolConfig={
                "tools": [{"toolSpec": schema} for schema in TOOL_SCHEMAS],
                "toolChoice": {"auto": {}}   # LLM 自己决定是否调用工具
            }
        )

    def _extract_text(self, content: list) -> str:
        return " ".join(
            block.get("text", "")
            for block in content
            if "text" in block
        ).strip()

    def _extract_json(self, text: str) -> dict | None:
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            pass
        text = re.sub(r"```json\s*", "", text)
        text = re.sub(r"```\s*", "", text)
        depth, start = 0, -1
        for i, char in enumerate(text):
            if char == "{":
                if depth == 0:
                    start = i
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0 and start != -1:
                    try:
                        return json.loads(text[start:i+1])
                    except json.JSONDecodeError:
                        continue
        return None

    def _save_result(self, task_name: str, data: dict):
        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        filename = self.results_dir / f"{task_name}_{date_str}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"[{task_name}] 已保存到 {filename}")

    def _error_result(self, task_name: str, error_msg: str) -> dict:
        return {
            "_meta": {
                "task_name": task_name,
                "executed_at": datetime.now().isoformat(),
                "success": False,
                "error": error_msg
            }
        }
