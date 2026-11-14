"""最小 pydantic 桩：支持 BaseModel 关键字构造（注解因 future annotations 不求值）。"""


class BaseModel:
    def __init__(self, **data):
        for key, value in data.items():
            setattr(self, key, value)

    def dict(self):
        return dict(self.__dict__)
