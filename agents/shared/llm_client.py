from __future__ import annotations
import logging
import os
from typing import Any

import anthropic

logger = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


def call_with_tools(
    system: str,
    messages: list[dict],
    tools: list[dict],
    model: str = "claude-sonnet-4-6",
    max_tokens: int = 4096,
) -> anthropic.types.Message:
    client = get_client()
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
        tools=tools,
    )
    logger.debug("LLM response: stop_reason=%s, usage=%s", response.stop_reason, response.usage)
    return response


def extract_text(response: anthropic.types.Message) -> str:
    for block in response.content:
        if block.type == "text":
            return block.text
    return ""


def extract_tool_use(response: anthropic.types.Message) -> list[dict[str, Any]]:
    results = []
    for block in response.content:
        if block.type == "tool_use":
            results.append({"name": block.name, "input": block.input, "id": block.id})
    return results
