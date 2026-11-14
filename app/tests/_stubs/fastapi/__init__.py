"""最小 FastAPI 桩：保留路由装饰器、HTTPException 与 Query 的运行期行为。"""


class FastAPI:
    def __init__(self, *args, **kwargs):
        self.routes = []

    def get(self, path, **kwargs):
        def decorator(func):
            self.routes.append((path, func))
            return func
        return decorator


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def Query(default=None, **kwargs):  # noqa: N802 - 对齐上游命名
    return default
