"""实现：迟到升格回放（切流账册形态）

形态：按原事件时重判目标窗——
- 命中：同一事务内 入窗缓冲增行 + 迟到匣删行 + 升格归档钉指纹（读匣写缓冲清匣同成败）
- 出窗：原样留匣，不增缓冲、不归档
- 模块级 RLock + SQLite BEGIN IMMEDIATE：升格与新迟到/补灌并发不双计、不双成功
- 禁止只改匣角标：现行匣删行后，行只存在于缓冲与升格归档，两路可区分

旁路回放仍属轨迹入库编排：本模块只重判已在迟到匣的轨迹，不接管装载主链。
"""
from __future__ import annotations
import threading
import uuid

from app.db import 连接
from app.seed import 此刻
from app.services import 目录, 时间

NAME = "迟到升格回放"
CONTRACT = "迟到匣按原事件时重判窗升格回放，读匣写缓冲清匣同成败，钉升格指纹"

HOOKS = (
    "before_load",
    "after_load",
    "decide_window",
    "before_close",
    "after_close",
    "extra_verify_checks",
)

# 升格临界区：装载（新迟到/补灌）与升格共用同一把进程内锁
升格锁 = threading.RLock()

_建归档表 = """
CREATE TABLE IF NOT EXISTS 升格归档 (
  归档键 TEXT PRIMARY KEY,
  事件编号 TEXT NOT NULL,
  运单键 TEXT NOT NULL,
  原事件时 TEXT NOT NULL,
  目标窗号 TEXT NOT NULL,
  目标窗起 TEXT NOT NULL,
  目标窗止 TEXT NOT NULL,
  升格指纹 TEXT NOT NULL,
  升格时 TEXT NOT NULL
)
"""


def describe() -> dict:
    return {
        "模块": NAME,
        "契约": CONTRACT,
        "形态": "切流账册升格回放——按原事件时重判窗，成功则现行缓冲增行匣删行并钉升格指纹",
        "钩子": list(HOOKS),
        "默认启用": True,
        "说明": "升格回放走同票事务；出窗留匣；并发由 RLock+BEGIN IMMEDIATE 串行化",
    }


def 取锁() -> threading.RLock:
    """暴露临界区锁：装载侧与测试侧用以验证并发不双计。"""
    return 升格锁


def _确保表(conn) -> None:
    conn.execute(_建归档表)


def _表存在(conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='升格归档'"
    ).fetchone() is not None


def _解析目标窗(目标窗: dict | None) -> dict:
    if not 目标窗:
        规 = 目录.窗规()
        目标窗 = {
            "窗号": 规.get("当前窗号", "W-demo"),
            "窗起": 规.get("窗起", ""),
            "窗止": 规.get("窗止", ""),
        }
    缺 = [k for k in ("窗号", "窗起", "窗止") if not 目标窗.get(k)]
    if 缺:
        raise ValueError(f"目标窗缺字段:{','.join(缺)}")
    for k in ("窗起", "窗止"):
        时间.解析时(str(目标窗[k]))
    if 时间.解析时(str(目标窗["窗起"])) >= 时间.解析时(str(目标窗["窗止"])):
        raise ValueError("目标窗窗起须早于窗止")
    return {
        "窗号": str(目标窗["窗号"]),
        "窗起": str(目标窗["窗起"]),
        "窗止": str(目标窗["窗止"]),
    }


def 升格只读(事件编号: str, 目标窗: dict | None = None) -> dict:
    """只读重判：回 事件编号/目标窗/归档键/valid，不写任何数据。

    - 在匣：按原事件时对目标窗现判 valid，归档键为 None
    - 已归档：valid=True，回读归档键，状态=已归档
    - 两路都无：valid=False，状态=未见
    """
    窗 = _解析目标窗(目标窗)
    conn = 连接()
    try:
        归档 = None
        if _表存在(conn):
            归档 = conn.execute(
                "SELECT * FROM 升格归档 WHERE 事件编号=? ORDER BY 升格时 DESC LIMIT 1",
                (事件编号,),
            ).fetchone()
        if 归档:
            return {
                "事件编号": 事件编号,
                "目标窗": 窗["窗号"],
                "归档键": 归档["归档键"],
                "valid": True,
                "状态": "已归档",
                "原事件时": 归档["原事件时"],
                "升格指纹": 归档["升格指纹"],
            }
        匣行 = conn.execute(
            "SELECT * FROM 迟到匣 WHERE 事件编号=? ORDER BY 编号 DESC LIMIT 1",
            (事件编号,),
        ).fetchone()
        if not 匣行:
            return {
                "事件编号": 事件编号,
                "目标窗": 窗["窗号"],
                "归档键": None,
                "valid": False,
                "状态": "未见",
            }
        valid = 时间.在窗内(匣行["事件时"], 窗["窗起"], 窗["窗止"])
        return {
            "事件编号": 事件编号,
            "目标窗": 窗["窗号"],
            "归档键": None,
            "valid": bool(valid),
            "状态": "在匣",
            "原事件时": 匣行["事件时"],
        }
    finally:
        conn.close()


def 升格回放(目标窗: dict | None = None) -> dict:
    """把迟到匣按原事件时重判目标窗后升格回放（同票事务）。"""
    窗 = _解析目标窗(目标窗)
    时刻 = 此刻()
    with 升格锁:
        conn = 连接()
        try:
            _确保表(conn)
            conn.execute("BEGIN IMMEDIATE")
            匣行 = conn.execute("SELECT * FROM 迟到匣 ORDER BY 编号").fetchall()
            升格号 = []
            留匣号 = []
            for r in 匣行:
                if not 时间.在窗内(r["事件时"], 窗["窗起"], 窗["窗止"]):
                    留匣号.append(r["事件编号"])
                    continue
                # 幂等：已归档/已在缓冲的事件编号不再升格（不双成功）
                if conn.execute(
                    "SELECT 1 FROM 升格归档 WHERE 事件编号=?", (r["事件编号"],)
                ).fetchone():
                    留匣号.append(r["事件编号"])
                    continue
                if conn.execute(
                    "SELECT 1 FROM 入窗缓冲 WHERE 事件编号=?", (r["事件编号"],)
                ).fetchone():
                    留匣号.append(r["事件编号"])
                    continue
                # 迟到匣不存网点键，回观测点取原上报网点
                观 = conn.execute(
                    "SELECT 网点键 FROM 观测点 WHERE 事件编号=?", (r["事件编号"],)
                ).fetchone()
                网点键 = 观["网点键"] if 观 else ""
                归档键 = f"档-{窗['窗号']}-{r['事件编号']}-{uuid.uuid4().hex[:8]}"
                指纹 = f"fp-{r['事件编号']}-{r['事件时']}-{窗['窗号']}-{uuid.uuid4().hex[:10]}"
                # 1) 现行缓冲增行（保留原事件时，不拿处理时顶替）
                conn.execute(
                    """INSERT INTO 入窗缓冲(事件编号,运单键,网点键,事件时,窗号,命运,入缓冲时)
                       VALUES (?,?,?,?,?,?,?)""",
                    (r["事件编号"], r["运单键"], 网点键, r["事件时"], 窗["窗号"], "在缓冲", 时刻),
                )
                # 2) 钉升格指纹（归档只读轨）
                conn.execute(
                    """INSERT INTO 升格归档(归档键,事件编号,运单键,原事件时,目标窗号,目标窗起,目标窗止,升格指纹,升格时)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (归档键, r["事件编号"], r["运单键"], r["事件时"],
                     窗["窗号"], 窗["窗起"], 窗["窗止"], 指纹, 时刻),
                )
                # 3) 现行匣删行（禁止只改角标：删行而非打标）
                conn.execute("DELETE FROM 迟到匣 WHERE 编号=?", (r["编号"],))
                conn.execute(
                    "UPDATE 观测点 SET 命运=? WHERE 事件编号=?",
                    ("已升格入缓冲", r["事件编号"]),
                )
                升格号.append(r["事件编号"])
            conn.commit()
            return {
                "目标窗": 窗["窗号"],
                "窗起": 窗["窗起"],
                "窗止": 窗["窗止"],
                "升格": len(升格号),
                "留匣": len(留匣号),
                "升格事件": 升格号,
                "留匣事件": 留匣号,
                "时刻": 时刻,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def 归档列表() -> list[dict]:
    conn = 连接()
    try:
        if not _表存在(conn):
            return []
        return [dict(r) for r in conn.execute(
            "SELECT * FROM 升格归档 ORDER BY 升格时 DESC, 归档键 DESC LIMIT 200"
        ).fetchall()]
    finally:
        conn.close()


def before_load(batch: dict) -> dict:
    return batch


def after_load(report: dict) -> dict:
    return report


def decide_window(event: dict, rules: dict) -> str:
    return "入缓冲"


def before_close(window_id: str) -> None:
    return None


def after_close(report: dict) -> dict:
    return report


def extra_verify_checks(snapshot: dict) -> list[str]:
    """升格一致性：归档事件须都能在现行缓冲/物化轨找到，且不在现行匣。"""
    conn = 连接()
    错: list[str] = []
    try:
        if not _表存在(conn):
            return []
        for a in conn.execute("SELECT 事件编号 FROM 升格归档").fetchall():
            在缓冲 = conn.execute(
                "SELECT 1 FROM 入窗缓冲 WHERE 事件编号=?", (a["事件编号"],)
            ).fetchone()
            在匣 = conn.execute(
                "SELECT 1 FROM 迟到匣 WHERE 事件编号=?", (a["事件编号"],)
            ).fetchone()
            if not 在缓冲:
                错.append(f"升格撕裂：归档 {a['事件编号']} 不在入窗缓冲")
            if 在匣:
                错.append(f"升格撕裂：{a['事件编号']} 同时存在于迟到匣与归档")
    finally:
        conn.close()
    return 错
