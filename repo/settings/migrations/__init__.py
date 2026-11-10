from collections.abc import Callable
from typing import Any

# key = 源版本号，value = 迁移到 key+1 的函数
REGISTRY: dict[int, Callable[[dict[str, Any]], dict[str, Any]]] = {}

__all__ = ["REGISTRY"]
