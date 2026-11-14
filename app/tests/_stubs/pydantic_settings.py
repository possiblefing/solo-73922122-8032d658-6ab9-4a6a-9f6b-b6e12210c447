"""最小 pydantic_settings 桩。"""
from pydantic import BaseModel


def SettingsConfigDict(**kwargs):  # noqa: N802 - 对齐上游命名
    return dict(kwargs)


class BaseSettings(BaseModel):
    def __init__(self, **data):
        super().__init__(**data)
