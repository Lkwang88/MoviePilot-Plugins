# -*- coding: utf-8 -*-
"""真实 Pydantic 环境下的归属标记保真测试（核心机制验证）"""
import copy
import unittest

try:
    import pydantic
    HAVE_PYDANTIC = True
except ImportError:  # pragma: no cover
    HAVE_PYDANTIC = False


@unittest.skipUnless(HAVE_PYDANTIC, "需要 pydantic")
class PydanticMarkerFidelity(unittest.TestCase):
    """验证「私有属性挂标记」在真实 pydantic v2 模型上的行为"""

    def _model(self):
        class Notif(pydantic.BaseModel):
            title: str = ""
            userid: object = None
            buttons: object = None
            force_reply: bool = False

        return Notif(title="t")

    def test_marker_set_get(self):
        m = self._model()
        object.__setattr__(m, "_otr_owner", "SeedWatch")
        self.assertEqual(getattr(m, "_otr_owner", None), "SeedWatch")

    def test_marker_not_in_dump(self):
        """标记不参与序列化：不入库、不进事件数据"""
        m = self._model()
        object.__setattr__(m, "_otr_owner", "SeedWatch")
        dump = m.model_dump()
        self.assertNotIn("_otr_owner", dump)
        self.assertNotIn("_otr_owner", m.to_json() if hasattr(m, "to_json") else "")

    def test_marker_survives_deepcopy(self):
        m = self._model()
        object.__setattr__(m, "_otr_owner", "SeedWatch")
        d = copy.deepcopy(m)
        self.assertEqual(getattr(d, "_otr_owner", None), "SeedWatch")

    def test_plain_getattr_missing_returns_default(self):
        m = self._model()
        self.assertIsNone(getattr(m, "_otr_owner", None))


if __name__ == "__main__":
    unittest.main()
