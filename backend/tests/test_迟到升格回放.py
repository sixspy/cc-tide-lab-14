"""迟到升格回放：读匣写缓冲清匣同成败、按原事件时重判窗、并发不双计。"""
from pathlib import Path
import os
import tempfile
import threading

import pytest
from starlette.testclient import TestClient

# 与 test_主链 同一测试库目录：db.库路径 在首次导入时定死，mkdtemp 各建各的会撕裂
os.environ.setdefault("数据目录", str(Path(tempfile.gettempdir()) / "tide-test-db"))
os.environ["夹具目录"] = str(Path(__file__).resolve().parents[2] / "fixtures")

from app import seed
from app.main import app
from app.db import 连接
from app.services import 装载, 关窗, 贯验, 目录, 夹具
from app.modules import 迟到升格回放 as 升格


@pytest.fixture(autouse=True)
def _db():
    conn = 连接()
    try:
        for t in ("窗贯验", "关窗批次", "装载批次", "入窗缓冲", "窗切片账", "迟到匣", "观测点", "升格归档", "窗规", "网点源"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit()
    finally:
        conn.close()
    seed.初始化库()
    yield


def _目标窗(起="2026-11-24T01:00:00Z", 止="2026-11-24T02:00:00Z", 号="W-next"):
    return {"窗号": 号, "窗起": 起, "窗止": 止}


def test_先迟到再升格入正确窗成片():
    # 夹具①：先迟到（事件时落在默认窗 W-demo 之外）
    报告 = 装载.装一批(夹具.读取("升格入窗样例"))
    assert 报告["迟到"] == 2
    assert len(目录.迟到列表()) == 2

    # 只读接口：升格前仍在现行匣，valid 已按原事件时重判为真
    只 = 升格.升格只读("E-up-1", _目标窗())
    assert 只["valid"] is True
    assert 只["目标窗"] == "W-next"
    assert 只["归档键"] is None
    assert 只["事件编号"] == "E-up-1"

    # 升格：按原事件时重判下一窗，命中 → 缓冲增行、匣删行、钉指纹
    回 = 升格.升格回放(目标窗=_目标窗())
    assert 回["升格"] == 2
    assert 回["留匣"] == 0
    assert len(目录.迟到列表()) == 0
    缓冲 = {r["事件编号"]: r for r in 目录.缓冲列表()}
    assert set(缓冲) == {"E-up-1", "E-up-2"}
    assert all(r["窗号"] == "W-next" and r["事件时"].startswith("2026-11-24T01:") for r in 缓冲.values())

    # 升格后现行匣不含该行，且可与归档区分
    只2 = 升格.升格只读("E-up-1", _目标窗())
    assert 只2["valid"] is True
    assert 只2["归档键"]
    assert 只2["状态"] == "已归档"
    归档 = 升格.归档列表()
    assert {a["事件编号"] for a in 归档} == {"E-up-1", "E-up-2"}
    assert all(a["升格指纹"] for a in 归档)

    # 升格进的行能随下一窗关窗成片
    目录.改窗规({"当前窗号": "W-next", "窗起": "2026-11-24T01:00:00Z", "窗止": "2026-11-24T02:00:00Z"})
    关 = 关窗.关当前窗()
    assert 关["成片数"] == 1
    assert 贯验.跑一次()["是否对齐"] is True


def test_升格仍出窗留匣或归档():
    # 夹具②：事件时 03:30，对目标窗 [01:00,02:00) 仍出窗
    装载.装一批(夹具.读取("升格出窗留匣样例"))
    只 = 升格.升格只读("E-stay-1", _目标窗())
    assert 只["valid"] is False
    assert 只["状态"] == "在匣"

    回 = 升格.升格回放(目标窗=_目标窗())
    assert 回["升格"] == 0
    assert 回["留匣"] == 1
    assert len(目录.迟到列表()) == 1  # 留匣
    assert 升格.归档列表() == []      # 未归档
    assert 目录.缓冲列表() == []      # 缓冲无增行

    # 改判到覆盖 03:30 的窗后再升格即成功，归档键可回读
    回2 = 升格.升格回放(目标窗=_目标窗(起="2026-11-24T03:00:00Z", 止="2026-11-24T04:00:00Z", 号="W-later"))
    assert 回2["升格"] == 1
    assert 升格.升格只读("E-stay-1", {"窗号": "W-later", "窗起": "2026-11-24T03:00:00Z", "窗止": "2026-11-24T04:00:00Z"})["归档键"]


def test_升格与新灌并发不得双计():
    # 夹具③：一条能升格（01:05），一条对目标窗出窗（05:05）
    装载.装一批(夹具.读取("升格并发样例"))
    目标 = _目标窗()
    屏障 = threading.Barrier(2)
    升格结果, 装载结果 = [], []

    def 升格线程():
        屏障.wait()
        升格结果.append(升格.升格回放(目标窗=目标))

    def 补灌线程():
        屏障.wait()
        装载结果.append(装载.装一批({
            "批次编号": "并发补灌",
            "事件": [{"事件编号": "E-par-1", "运单键": "运单-R", "网点键": "网点-东站", "事件时": "2026-11-24T01:05:00Z"}],
        }))

    t1 = threading.Thread(target=升格线程)
    t2 = threading.Thread(target=补灌线程)
    t1.start(); t2.start()
    t1.join(); t2.join()

    # 同一把锁串行化：升格要么先成（补灌被幂等驳回），要么补灌先驳回（升格仍只成一次）
    assert 升格结果[0]["升格"] == 1
    assert 装载结果[0]["驳回"] == 1  # 观测点主键幂等，绝不双计

    # 同一行二次升格不得再成功（不双成功）
    二 = 升格.升格回放(目标窗=目标)
    assert 二["升格"] == 0
    assert 二["留匣"] == 1  # 剩 05:05 那条仍在匣
    conn = 连接()
    try:
        缓冲行 = conn.execute("SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 事件编号='E-par-1'").fetchone()["c"]
        归档行 = conn.execute("SELECT COUNT(*) AS c FROM 升格归档 WHERE 事件编号='E-par-1'").fetchone()["c"]
        匣行 = conn.execute("SELECT COUNT(*) AS c FROM 迟到匣 WHERE 事件编号='E-par-1'").fetchone()["c"]
    finally:
        conn.close()
    assert 缓冲行 == 1
    assert 归档行 == 1
    assert 匣行 == 0  # 升格后现行匣不含该行
    assert 贯验.跑一次()["是否对齐"] is True


def test_读匣写缓冲清匣同成败回滚():
    装载.装一批(夹具.读取("升格入窗样例"))
    # 目标窗缺窗止 → 重判前即抛错，匣/缓冲/归档均无变化
    with pytest.raises(ValueError):
        升格.升格回放(目标窗={"窗号": "W-bad", "窗起": "2026-11-24T01:00:00Z"})
    assert len(目录.迟到列表()) == 2
    assert 目录.缓冲列表() == []
    assert 升格.归档列表() == []


def test_升格只读接口四字段():
    装载.装一批(夹具.读取("升格出窗留匣样例"))
    只 = 升格.升格只读("E-stay-1", _目标窗())
    assert set(["事件编号", "目标窗", "归档键", "valid"]).issubset(只.keys())
    assert 只["目标窗"] == "W-next"


def test_升格贯验扩展检撕裂():
    装载.装一批(夹具.读取("升格入窗样例"))
    目标 = _目标窗()
    assert 升格.升格回放(目标窗=目标)["升格"] == 2
    # 健康态：归档都能在缓冲找到、且不在匣
    assert 升格.extra_verify_checks({}) == []

    conn = 连接()
    try:
        # 人为撕裂①：删缓冲行但留归档 → 应检出
        conn.execute("DELETE FROM 入窗缓冲 WHERE 事件编号='E-up-1'")
        conn.commit()
        错 = 升格.extra_verify_checks({})
        assert any("E-up-1" in e and "不在入窗缓冲" in e for e in 错)

        # 人为撕裂②：把已升格行塞回匣 → 应检出双轨并存
        conn.execute(
            "INSERT INTO 迟到匣(事件编号,运单键,原因,事件时,入匣时) VALUES (?,?,?,?,?)",
            ("E-up-1", "运单-P", "人为回塞", "2026-11-24T01:12:00Z", "2026-11-24T01:12:00Z"),
        )
        conn.commit()
    finally:
        conn.close()
    错2 = 升格.extra_verify_checks({})
    assert any("同时存在于迟到匣与归档" in e for e in 错2)


def test_升格路由只读与回放():
    装载.装一批(夹具.读取("升格入窗样例"))
    with TestClient(app) as c:
        r = c.get("/api/迟到匣/升格只读", params={"事件编号": "E-up-1", "窗号": "W-next", "窗起": "2026-11-24T01:00:00Z", "窗止": "2026-11-24T02:00:00Z"})
        assert r.status_code == 200
        assert r.json()["valid"] is True
        r2 = c.post("/api/迟到匣/升格回放", json={"目标窗": {"窗号": "W-next", "窗起": "2026-11-24T01:00:00Z", "窗止": "2026-11-24T02:00:00Z"}})
        assert r2.status_code == 200
        assert r2.json()["升格"] == 2
        r3 = c.get("/api/迟到匣/升格归档")
        assert r3.status_code == 200
        assert len(r3.json()) == 2


def test_基线主链不被改坏():
    体 = 夹具.读取("窗内成片样例")
    报告 = 装载.装一批(体)
    assert 报告["入缓冲"] == 3
    关 = 关窗.关当前窗()
    assert 关["成片数"] == 2
    assert 贯验.跑一次()["是否对齐"] is True
