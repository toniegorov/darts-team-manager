"""
Дартс · Командный менеджер  (PySide6 edition)
====================================================
Правила:
  - 9 основных игр: 3 на максимум, 3 на минимум, 3 на бул
  - Бул: значения от -25 до 150, просто суммируются
  - При ничьей — «Доп. игра (по 1 дротику)»
  - Ввод «x» = пропуск (не считается в сумму и статистику)
  - Отрицательные значения разрешены только в бул
  - Сектора: 2 дополнительных игры, +1 к общему счёту каждая
  - Ручное сохранение (кнопка), множественное удаление истории

Зависимости:  pip install PySide6 matplotlib
"""

import json
import sqlite3
import os
import sys
import re
import ast
import operator
import random
from datetime import datetime
from statistics import mean
from collections import defaultdict
from typing import Optional, Any

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QLabel, QPushButton, QLineEdit, QCheckBox, QComboBox,
    QFrame, QScrollArea, QTreeWidget, QTreeWidgetItem, QHeaderView,
    QSplitter, QDialog, QDialogButtonBox, QMessageBox, QColorDialog,
    QAbstractItemView, QSizePolicy, QSpacerItem, QGroupBox
)
from PySide6.QtCore import Qt, QTimer, Signal, QSize
from PySide6.QtGui import QColor, QFont, QIcon, QKeySequence, QShortcut

try:
    import matplotlib
    matplotlib.use('QtAgg')
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
    from matplotlib.figure import Figure
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ═══════════════════════════════════════════════════════════════════════════════
#  Константы
# ═══════════════════════════════════════════════════════════════════════════════

SKIP = "__skip__"

PALETTE = ["#E06C75", "#61AFEF", "#98C379", "#E5C07B", "#C678DD"]

ROUND_DEFS = [
    ("Больше", "max"), ("Больше", "max"), ("Больше", "max"),
    ("Меньше", "min"), ("Меньше", "min"), ("Меньше", "min"),
    ("Бул", "bull"), ("Бул", "bull"), ("Бул", "bull"),
]

ROW_TITLES = [
    "▲  3 игры на максимум",
    "▼  3 игры на минимум",
    "●  3 игры на бул",
]

ROW_COLORS = ["#98C379", "#E5C07B", "#C678DD"]

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "calculation.db")

# ═══════════════════════════════════════════════════════════════════════════════
#  Утилиты
# ═══════════════════════════════════════════════════════════════════════════════

def is_skip(txt: str) -> bool:
    return txt.strip().lower() in ("x", "х")


def safe_mean(lst):
    vals = [v for v in lst if v is not None and v != SKIP]
    return round(mean(vals), 1) if vals else None


# Safe math expression evaluator (replaces eval)
_SAFE_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}

def _safe_eval_node(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_safe_eval_node(node.operand)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd):
        return _safe_eval_node(node.operand)
    if isinstance(node, ast.BinOp):
        op_func = _SAFE_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError("Unsupported operator")
        left = _safe_eval_node(node.left)
        right = _safe_eval_node(node.right)
        if isinstance(node.op, (ast.FloorDiv, ast.Mod)) and right == 0:
            return 0
        return op_func(left, right)
    raise ValueError("Unsupported expression")


def eval_expression(txt: str, allow_negative: bool = False) -> int:
    txt = txt.strip().replace(" ", "")
    if not txt:
        return 0
    try:
        tree = ast.parse(txt, mode='eval')
        result = int(_safe_eval_node(tree.body))
        if not allow_negative and result < 0:
            return 0
        return result
    except Exception:
        return 0


def dense_rank(mapping: dict, reverse=True):
    if not mapping:
        return {}
    items = sorted(mapping.items(), key=lambda x: x[1], reverse=reverse)
    ranks = {}
    rank = 1
    for i, (k, v) in enumerate(items):
        if i > 0 and v != items[i - 1][1]:
            rank = i + 1
        ranks[k] = rank
    return ranks


def fmt(v):
    if v is None:
        return "—"
    return str(v)


# ═══════════════════════════════════════════════════════════════════════════════
#  База данных (идентична оригиналу для совместимости)
# ═══════════════════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path=DB_PATH):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        c = self.conn
        c.execute("""CREATE TABLE IF NOT EXISTS persons (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS matches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        played_at TEXT,
                        winner TEXT,
                        reason TEXT,
                        summary TEXT,
                        payload TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS kv (
                        key TEXT PRIMARY KEY,
                        value TEXT)""")
        c.commit()

    # --- Persons ---
    def all_persons(self):
        return [dict(r) for r in
                self.conn.execute("SELECT id, name FROM persons ORDER BY id")]

    def add_person(self, name):
        self.conn.execute("INSERT INTO persons (name) VALUES (?)", (name,))
        self.conn.commit()

    def rename_person(self, pid, name):
        self.conn.execute("UPDATE persons SET name=? WHERE id=?", (name, pid))
        self.conn.commit()

    def remove_person(self, pid):
        self.conn.execute("DELETE FROM persons WHERE id=?", (pid,))
        self.conn.commit()

    # --- Matches ---
    def save_match(self, payload: dict):
        self.conn.execute(
            "INSERT INTO matches (played_at, winner, reason, summary, payload)"
            " VALUES (?,?,?,?,?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M"),
             payload.get("winner"),
             payload.get("reason"),
             payload.get("summary"),
             json.dumps(payload, ensure_ascii=False)))
        self.conn.commit()

    def all_matches(self):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM matches ORDER BY id DESC")]

    def match_by_id(self, mid):
        r = self.conn.execute(
            "SELECT * FROM matches WHERE id=?", (mid,)).fetchone()
        return dict(r) if r else None

    def delete_matches(self, ids):
        if not ids:
            return
        ph = ",".join("?" * len(ids))
        self.conn.execute(f"DELETE FROM matches WHERE id IN ({ph})", ids)
        self.conn.commit()

    # --- KV ---
    def save_state(self, key, value):
        self.conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?,?)",
            (key, json.dumps(value, ensure_ascii=False)))
        self.conn.commit()

    def load_state(self, key, default=None):
        r = self.conn.execute(
            "SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        if r:
            try:
                return json.loads(r["value"])
            except (json.JSONDecodeError, TypeError):
                pass
        return default

    # --- Stats ---
    def calc_stats(self):
        matches = self.all_matches()
        data = defaultdict(lambda: {"name": "", "n": 0,
                                     "max": [], "min": [], "bull": []})
        for m in matches:
            try:
                p = json.loads(m["payload"])
            except Exception:
                continue
            seen = set()
            for rnd in p.get("rounds", []):
                mode = rnd.get("mode", "max")
                for ti_str, smap in rnd.get("scores", {}).items():
                    for pid_str, val in smap.items():
                        pid = int(pid_str)
                        if val == SKIP or val == "x":
                            continue
                        try:
                            v = int(val) if not isinstance(val, int) else val
                        except (ValueError, TypeError):
                            continue
                        seen.add(pid)
                        data[pid][mode].append(v)
            for pid in seen:
                data[pid]["n"] += 1

        persons = {r["id"]: r["name"] for r in self.all_persons()}
        for pid in data:
            data[pid]["name"] = persons.get(pid, str(pid))

        avg_max_map = {pid: safe_mean(d["max"]) for pid, d in data.items()
                       if safe_mean(d["max"]) is not None}
        avg_min_map = {pid: safe_mean(d["min"]) for pid, d in data.items()
                       if safe_mean(d["min"]) is not None}
        avg_bull_map = {pid: safe_mean(d["bull"]) for pid, d in data.items()
                        if safe_mean(d["bull"]) is not None}

        rank_max = dense_rank(avg_max_map, reverse=True)
        rank_min = dense_rank(avg_min_map, reverse=False)
        rank_bull = dense_rank(avg_bull_map, reverse=True)

        rows = []
        for pid, d in data.items():
            places = [rank_max.get(pid), rank_min.get(pid), rank_bull.get(pid)]
            places = [p for p in places if p is not None]
            avg_place = round(mean(places), 2) if places else None
            rows.append({
                "name": d["name"], "n": d["n"],
                "avg_max": safe_mean(d["max"]),
                "avg_min": safe_mean(d["min"]),
                "avg_bull": safe_mean(d["bull"]),
                "r_max": rank_max.get(pid),
                "r_min": rank_min.get(pid),
                "r_bull": rank_bull.get(pid),
                "avg_p": avg_place,
            })

        if rows:
            ap_map = {i: r["avg_p"] for i, r in enumerate(rows)
                      if r["avg_p"] is not None}
            overall = dense_rank(ap_map, reverse=False)
            for i, r in enumerate(rows):
                r["overall"] = overall.get(i)
        return rows

    def get_player_history(self, pid):
        matches = self.all_matches()
        history = []
        for m in reversed(matches):  # chronological order (oldest first)
            try:
                p = json.loads(m["payload"])
            except Exception:
                continue
            match_data = {"id": m["id"], "played_at": m["played_at"],
                          "max": None, "min": None, "bull": None}
            for rnd in p.get("rounds", []):
                mode = rnd.get("mode", "max")
                scores = rnd.get("scores", {})
                for ti_str, smap in scores.items():
                    if str(pid) in smap:
                        val = smap[str(pid)]
                        if val != SKIP and val != "x":
                            try:
                                v = int(val) if not isinstance(val, int) else val
                                if match_data[mode] is None:
                                    match_data[mode] = []
                                match_data[mode].append(v)
                            except (ValueError, TypeError):
                                pass
            if match_data["max"] or match_data["min"] or match_data["bull"]:
                match_data["avg_max"] = round(mean(match_data["max"]), 1) if match_data["max"] else None
                match_data["avg_min"] = round(mean(match_data["min"]), 1) if match_data["min"] else None
                match_data["avg_bull"] = round(mean(match_data["bull"]), 1) if match_data["bull"] else None
                history.append(match_data)
        return history


# ═══════════════════════════════════════════════════════════════════════════════
#  Тёмная тема (Catppuccin Macchiato)
# ═══════════════════════════════════════════════════════════════════════════════

DARK_QSS = """
QMainWindow, QWidget {
    background-color: #24273A;
    color: #CDD6F4;
    font-family: "Segoe UI", "Ubuntu", sans-serif;
    font-size: 13px;
}
QTabWidget::pane {
    border: 1px solid #363848;
    border-radius: 8px;
    background-color: #24273A;
}
QTabBar::tab {
    background-color: #363848;
    color: #CDD6F4;
    padding: 8px 20px;
    margin-right: 2px;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    font-size: 14px;
    font-weight: bold;
}
QTabBar::tab:selected {
    background-color: #494D64;
    color: #FFFFFF;
}
QTabBar::tab:hover {
    background-color: #444660;
}
QFrame, QGroupBox {
    background-color: #2B2D3A;
    border-radius: 8px;
}
QGroupBox {
    font-weight: bold;
    padding-top: 16px;
    margin-top: 8px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    padding: 0 8px;
}
QLabel {
    background-color: transparent;
    border: none;
}
QLineEdit {
    background-color: #363848;
    color: #CDD6F4;
    border: 1px solid #494D64;
    border-radius: 6px;
    padding: 4px 8px;
    font-size: 13px;
    selection-background-color: #61AFEF;
    selection-color: #000000;
}
QLineEdit:focus {
    border: 1px solid #61AFEF;
}
QPushButton {
    background-color: #494D64;
    color: #CDD6F4;
    border: none;
    border-radius: 8px;
    padding: 6px 16px;
    font-size: 13px;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #5B5F77;
}
QPushButton:pressed {
    background-color: #61AFEF;
    color: #000000;
}
QPushButton:disabled {
    background-color: #363848;
    color: #555555;
}
QComboBox {
    background-color: #363848;
    color: #CDD6F4;
    border: 1px solid #494D64;
    border-radius: 6px;
    padding: 4px 8px;
    font-size: 13px;
}
QComboBox::drop-down {
    border: none;
    width: 20px;
}
QComboBox QAbstractItemView {
    background-color: #363848;
    color: #CDD6F4;
    selection-background-color: #61AFEF;
    selection-color: #000000;
    border: 1px solid #494D64;
}
QCheckBox {
    background-color: transparent;
    spacing: 6px;
    font-size: 13px;
}
QCheckBox::indicator {
    width: 18px;
    height: 18px;
    border: 2px solid #494D64;
    border-radius: 4px;
    background-color: #363848;
}
QCheckBox::indicator:checked {
    background-color: #61AFEF;
    border-color: #61AFEF;
}
QTreeWidget {
    background-color: #2B2D3A;
    color: #CDD6F4;
    border: none;
    font-size: 12px;
    alternate-background-color: #303347;
}
QTreeWidget::item {
    padding: 4px;
    border: none;
}
QTreeWidget::item:selected {
    background-color: #61AFEF;
    color: #000000;
}
QTreeWidget::item:hover {
    background-color: #363848;
}
QHeaderView::section {
    background-color: #363848;
    color: #CDD6F4;
    padding: 6px;
    border: none;
    font-weight: bold;
    font-size: 12px;
}
QScrollArea {
    background-color: transparent;
    border: none;
}
QScrollBar:vertical {
    background-color: #2B2D3A;
    width: 10px;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background-color: #494D64;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover {
    background-color: #61AFEF;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}
QScrollBar:horizontal {
    background-color: #2B2D3A;
    height: 10px;
    border-radius: 5px;
}
QScrollBar::handle:horizontal {
    background-color: #494D64;
    border-radius: 5px;
    min-width: 30px;
}
QScrollBar::handle:horizontal:hover {
    background-color: #61AFEF;
}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}
QSplitter::handle {
    background-color: #494D64;
    width: 2px;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Вспомогательные виджеты
# ═══════════════════════════════════════════════════════════════════════════════

class ColorButton(QPushButton):
    """Кнопка выбора цвета."""
    colorChanged = Signal(str)

    def __init__(self, color="#888888", parent=None):
        super().__init__(parent)
        self._color = color
        self.setFixedSize(32, 32)
        self._update_style()
        self.clicked.connect(self._pick)

    def _update_style(self):
        self.setStyleSheet(
            f"QPushButton {{ background-color: {self._color}; border-radius: 8px; border: none; }}"
            f"QPushButton:hover {{ background-color: {self._color}; border: 2px solid #FFFFFF; }}")

    def color(self):
        return self._color

    def setColor(self, c):
        self._color = c
        self._update_style()

    def _pick(self):
        c = QColorDialog.getColor(QColor(self._color), self, "Цвет команды")
        if c.isValid():
            self._color = c.name()
            self._update_style()
            self.colorChanged.emit(self._color)


class ScoreEntry(QLineEdit):
    """Поле ввода очков с поддержкой выражений и навигации."""
    def __init__(self, width=60, parent=None):
        super().__init__(parent)
        self.setFixedWidth(width)
        self.setFixedHeight(28)
        self.setAlignment(Qt.AlignCenter)

    def focusOutEvent(self, event):
        self._eval_expression()
        super().focusOutEvent(event)

    def keyPressEvent(self, event):
        key = event.key()
        if key == Qt.Key_Return or key == Qt.Key_Enter:
            self._eval_expression()
            self.focusNextChild()
            return
        if key == Qt.Key_Down:
            self._eval_expression()
            self.focusNextChild()
            return
        if key == Qt.Key_Up:
            self._eval_expression()
            self.focusPreviousChild()
            return
        # Left/Right — only cursor movement, do NOT override
        super().keyPressEvent(event)

    def _eval_expression(self):
        txt = self.text().strip()
        if txt and not is_skip(txt):
            result = eval_expression(txt)
            if result != 0 or txt in ("0", "0+0"):
                self.setText(str(result))


# ═══════════════════════════════════════════════════════════════════════════════
#  Главное приложение (PySide6)
# ═══════════════════════════════════════════════════════════════════════════════

class App(QMainWindow):

    def __init__(self):
        super().__init__()

        self.setWindowTitle("Дартс · Командный менеджер")
        self.resize(1500, 920)
        self.setMinimumSize(1200, 700)

        self.db = DB()
        self.persons = {}
        self.team_members = {}
        self.round_widgets = []
        self.sectors_var = False
        self.sectors_ui = None
        self.payload = None
        self.saved_sig = None
        self._recalc_timer = QTimer(self)
        self._recalc_timer.setSingleShot(True)
        self._recalc_timer.setInterval(80)
        self._recalc_timer.timeout.connect(self._recalc)

        # Team configuration
        self.team_cfg = self.db.load_state("team_cfg", {
            "count": 2,
            "teams": [{"name": "Команда 1", "color": PALETTE[0]},
                      {"name": "Команда 2", "color": PALETTE[1]}],
        })
        self._ensure_cfg()

        self.team_vars = []  # list of dicts: {"name": str, "color": str, "name_edit": QLineEdit, "color_btn": ColorButton}
        self.save_teams = self.db.load_state("save_teams", False)

        self._build()
        self._refresh_persons()
        if self.save_teams:
            self._load_members()
        self._rebuild_team_lists()
        self._build_match()
        self._refresh_history()

    # ─── Конфигурация команд ──────────────────────────────────────────────

    def _ensure_cfg(self):
        cnt = max(2, min(5, int(self.team_cfg.get("count", 2))))
        teams = list(self.team_cfg.get("teams", []))
        while len(teams) < cnt:
            i = len(teams)
            teams.append({"name": f"Команда {i+1}",
                          "color": PALETTE[i % len(PALETTE)]})
        self.team_cfg = {"count": cnt, "teams": teams[:cnt]}

    def _vars_to_cfg(self):
        teams = []
        for tv in self.team_vars:
            teams.append({"name": tv["name_edit"].text(),
                          "color": tv["color_btn"].color()})
        self.team_cfg = {"count": len(teams), "teams": teams}
        self.db.save_state("team_cfg", self.team_cfg)

    def _save_members(self):
        if self.save_teams:
            self.db.save_state("team_members", self.team_members)
            self.db.save_state("save_teams", True)
        else:
            self.db.save_state("save_teams", False)

    def _load_members(self):
        raw = self.db.load_state("team_members", {})
        self.team_members = {int(k): v for k, v in raw.items()}
        valid = set(self.persons.keys())
        for ti in list(self.team_members.keys()):
            self.team_members[ti] = [p for p in self.team_members[ti]
                                     if p in valid]

    def team_name(self, ti):
        if 0 <= ti < len(self.team_vars):
            return self.team_vars[ti]["name_edit"].text()
        return f"Команда {ti+1}"

    def team_color(self, ti):
        if 0 <= ti < len(self.team_vars):
            return self.team_vars[ti]["color_btn"].color()
        return "#888888"

    def _schedule_recalc(self):
        self._recalc_timer.start()

    # ═══════════════════════════════════════════════════════════════════════
    #  Построение основного UI
    # ═══════════════════════════════════════════════════════════════════════

    def _build(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        main_layout.addWidget(self.tabs)

        self.tab_people = QWidget()
        self.tab_match = QWidget()
        self.tab_stats = QWidget()

        self.tabs.addTab(self.tab_people, "  Участники  ")
        self.tabs.addTab(self.tab_match, "  Матч  ")
        self.tabs.addTab(self.tab_stats, "  Статистика  ")

        self._build_people_tab()
        self._build_match_tab()
        self._build_stats_tab()

    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Участники»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_people_tab(self):
        layout = QVBoxLayout(self.tab_people)
        layout.setContentsMargins(8, 8, 8, 8)

        # --- Настройки команд ---
        cfg_frame = QFrame()
        cfg_layout = QVBoxLayout(cfg_frame)
        cfg_layout.setContentsMargins(12, 10, 12, 10)

        cfg_top = QHBoxLayout()
        cfg_top.addWidget(QLabel("Количество команд:"))

        self.count_combo = QComboBox()
        self.count_combo.addItems(["2", "3", "4", "5"])
        self.count_combo.setCurrentText(str(self.team_cfg["count"]))
        self.count_combo.setFixedWidth(70)
        self.count_combo.currentTextChanged.connect(self._on_count_change)
        cfg_top.addWidget(self.count_combo)

        cfg_top.addSpacing(20)

        self.save_teams_cb = QCheckBox("Сохранять составы")
        self.save_teams_cb.setChecked(self.save_teams)
        self.save_teams_cb.toggled.connect(self._on_save_teams_toggled)
        cfg_top.addWidget(self.save_teams_cb)
        cfg_top.addStretch()

        cfg_layout.addLayout(cfg_top)

        # Team name/color row
        self.cfg_host_layout = QHBoxLayout()
        cfg_layout.addLayout(self.cfg_host_layout)
        self._rebuild_cfg_row()

        layout.addWidget(cfg_frame)

        # --- Тело: свободные + команды ---
        body_splitter = QSplitter(Qt.Horizontal)

        # Свободные участники
        free_frame = QFrame()
        free_layout = QVBoxLayout(free_frame)
        free_layout.setContentsMargins(8, 8, 8, 8)

        lbl = QLabel("Свободные участники")
        lbl.setStyleSheet("font-size: 14px; font-weight: bold;")
        free_layout.addWidget(lbl)

        self.free_tree = QTreeWidget()
        self.free_tree.setHeaderLabels(["ID", "Имя"])
        self.free_tree.setColumnWidth(0, 40)
        self.free_tree.setColumnWidth(1, 150)
        self.free_tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.free_tree.setAlternatingRowColors(True)
        self.free_tree.setRootIsDecorated(False)
        free_layout.addWidget(self.free_tree)

        body_splitter.addWidget(free_frame)

        # Составы команд
        teams_frame = QFrame()
        teams_layout = QVBoxLayout(teams_frame)
        teams_layout.setContentsMargins(8, 8, 8, 8)

        lbl2 = QLabel("Составы команд")
        lbl2.setStyleSheet("font-size: 14px; font-weight: bold;")
        teams_layout.addWidget(lbl2)

        self.teams_host = QWidget()
        self.teams_host_layout = QHBoxLayout(self.teams_host)
        self.teams_host_layout.setContentsMargins(0, 0, 0, 0)
        teams_layout.addWidget(self.teams_host)

        body_splitter.addWidget(teams_frame)
        body_splitter.setStretchFactor(0, 1)
        body_splitter.setStretchFactor(1, 3)

        layout.addWidget(body_splitter, 1)

        # --- Панель действий ---
        bar = QFrame()
        bar_layout = QHBoxLayout(bar)
        bar_layout.setContentsMargins(12, 10, 12, 10)

        bar_layout.addWidget(QLabel("Имя:"))
        self.name_entry = QLineEdit()
        self.name_entry.setFixedWidth(160)
        self.name_entry.returnPressed.connect(self._add_person)
        bar_layout.addWidget(self.name_entry)
        bar_layout.addSpacing(12)

        btn_add = QPushButton("Добавить")
        btn_add.setFixedWidth(100)
        btn_add.clicked.connect(self._add_person)
        bar_layout.addWidget(btn_add)

        btn_rename = QPushButton("Переименовать")
        btn_rename.setFixedWidth(130)
        btn_rename.clicked.connect(self._rename_person)
        bar_layout.addWidget(btn_rename)

        btn_del = QPushButton("Удалить участника")
        btn_del.setFixedWidth(150)
        btn_del.setStyleSheet("QPushButton { background-color: #E06C75; } QPushButton:hover { background-color: #C85A63; }")
        btn_del.clicked.connect(self._del_person)
        bar_layout.addWidget(btn_del)

        # Separator
        sep = QFrame()
        sep.setFixedSize(2, 28)
        sep.setStyleSheet("background-color: #555555; border-radius: 0px;")
        bar_layout.addWidget(sep)

        btn_remove = QPushButton("Убрать из команды")
        btn_remove.setFixedWidth(150)
        btn_remove.clicked.connect(self._remove_from_team)
        bar_layout.addWidget(btn_remove)

        btn_random = QPushButton("Рандомное распределение")
        btn_random.setFixedWidth(200)
        btn_random.setStyleSheet("QPushButton { background-color: #E5C07B; color: #000000; } QPushButton:hover { background-color: #C9A63A; }")
        btn_random.clicked.connect(self._randomize_teams)
        bar_layout.addWidget(btn_random)

        btn_clear = QPushButton("Очистить все команды")
        btn_clear.setFixedWidth(180)
        btn_clear.setStyleSheet("QPushButton { background-color: #E06C75; } QPushButton:hover { background-color: #C85A63; }")
        btn_clear.clicked.connect(self._clear_teams)
        bar_layout.addWidget(btn_clear)

        bar_layout.addStretch()
        layout.addWidget(bar)

    def _rebuild_cfg_row(self):
        # Clear existing widgets
        while self.cfg_host_layout.count():
            item = self.cfg_host_layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()
            elif item.layout():
                self._clear_layout(item.layout())

        self.team_vars = []
        for i, t in enumerate(self.team_cfg["teams"]):
            f = QHBoxLayout()
            f.addWidget(QLabel(f"Команда {i+1}:"))
            ne = QLineEdit(t["name"])
            ne.setFixedWidth(130)
            ne.textChanged.connect(lambda _: self._vars_to_cfg())
            f.addWidget(ne)
            cb = ColorButton(t["color"])
            cb.colorChanged.connect(lambda _: self._vars_to_cfg())
            f.addWidget(cb)

            container = QWidget()
            container.setLayout(f)
            container.setStyleSheet("background-color: transparent;")
            self.cfg_host_layout.addWidget(container)

            self.team_vars.append({"name_edit": ne, "color_btn": cb})

        self.cfg_host_layout.addStretch()

    def _on_count_change(self, text):
        try:
            cnt = int(text)
        except ValueError:
            return
        cnt = max(2, min(5, cnt))
        old = len(self.team_cfg["teams"])
        if cnt == old:
            return

        teams = list(self.team_cfg["teams"])
        if cnt > old:
            for i in range(old, cnt):
                teams.append({"name": f"Команда {i+1}",
                              "color": PALETTE[i % len(PALETTE)]})
        else:
            teams = teams[:cnt]

        for ti in list(self.team_members.keys()):
            if ti >= cnt:
                del self.team_members[ti]

        self.team_cfg = {"count": cnt, "teams": teams}
        self.db.save_state("team_cfg", self.team_cfg)
        self._rebuild_cfg_row()
        self._rebuild_team_lists()

    def _on_save_teams_toggled(self, checked):
        self.save_teams = checked
        self._save_members()

    def _clear_layout(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()
            elif item.layout():
                self._clear_layout(item.layout())

    # ─── Участники: CRUD ─────────────────────────────────────────────────

    def _refresh_persons(self):
        self.persons = {int(r["id"]): r["name"]
                        for r in self.db.all_persons()}
        self._rebuild_free_tree()

    def _rebuild_free_tree(self):
        self.free_tree.clear()
        assigned = set()
        for pids in self.team_members.values():
            assigned.update(pids)
        for pid, name in self.persons.items():
            if pid not in assigned:
                item = QTreeWidgetItem([str(pid), name])
                item.setData(0, Qt.UserRole, pid)
                self.free_tree.addTopLevelItem(item)

    def _add_person(self):
        name = self.name_entry.text().strip()
        if not name:
            return
        self.db.add_person(name)
        self.name_entry.clear()
        self._refresh_persons()

    def _rename_person(self):
        # Try free tree first, then team trees
        sel = self.free_tree.selectedItems()
        pid = None
        if sel:
            pid = sel[0].data(0, Qt.UserRole)
        else:
            for ti, tree in getattr(self, 'team_trees', {}).items():
                s = tree.selectedItems()
                if s:
                    pid = s[0].data(0, Qt.UserRole)
                    break
        if pid is None:
            return
        new_name = self.name_entry.text().strip()
        if not new_name:
            QMessageBox.warning(self, "Ошибка", "Введите новое имя.")
            return
        self.db.rename_person(pid, new_name)
        self.name_entry.clear()
        self._refresh_persons()
        self._rebuild_team_lists()

    def _del_person(self):
        sel = self.free_tree.selectedItems()
        if not sel:
            return
        if QMessageBox.question(
                self, "Удаление",
                f"Удалить {len(sel)} участник(ов)?") != QMessageBox.Yes:
            return
        for item in sel:
            pid = item.data(0, Qt.UserRole)
            self.db.remove_person(pid)
            for ti in self.team_members:
                if pid in self.team_members[ti]:
                    self.team_members[ti].remove(pid)
        self._refresh_persons()
        self._rebuild_team_lists()

    # ─── Составы команд ──────────────────────────────────────────────────

    def _rebuild_team_lists(self):
        self._clear_layout(self.teams_host_layout)
        self.team_trees = {}
        cnt = len(self.team_vars)

        for ti in range(cnt):
            if ti not in self.team_members:
                self.team_members[ti] = []

            col = QFrame()
            col.setStyleSheet(f"QFrame {{ background-color: #2B2D3A; border-radius: 8px; }}")
            col_layout = QVBoxLayout(col)
            col_layout.setContentsMargins(8, 8, 8, 8)

            # Header
            hdr = QLabel(self.team_name(ti))
            hdr.setStyleSheet(f"font-size: 14px; font-weight: bold; color: {self.team_color(ti)};")
            col_layout.addWidget(hdr)

            # Tree
            tree = QTreeWidget()
            tree.setHeaderLabels(["Игрок"])
            tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
            tree.setAlternatingRowColors(True)
            tree.setRootIsDecorated(False)
            tree.header().setStretchLastSection(True)

            for pid in self.team_members[ti]:
                name = self.persons.get(pid, str(pid))
                item = QTreeWidgetItem([name])
                item.setData(0, Qt.UserRole, pid)
                tree.addTopLevelItem(item)

            col_layout.addWidget(tree)
            self.team_trees[ti] = tree

            # Buttons
            btn_row = QHBoxLayout()
            btn_add = QPushButton("Добавить в команду")
            btn_add.setFixedHeight(30)
            btn_add.clicked.connect(lambda checked, t=ti: self._add_to_team(t))
            btn_row.addWidget(btn_add)

            btn_rem = QPushButton("Удалить из команды")
            btn_rem.setFixedHeight(30)
            btn_rem.setStyleSheet("QPushButton { background-color: #E06C75; } QPushButton:hover { background-color: #C85A63; }")
            btn_rem.clicked.connect(lambda checked, t=ti: self._remove_from_team_specific(t))
            btn_row.addWidget(btn_rem)

            col_layout.addLayout(btn_row)
            self.teams_host_layout.addWidget(col)

        self._rebuild_free_tree()
        self._save_members()

    def _add_to_team(self, ti):
        sel = self.free_tree.selectedItems()
        if not sel:
            return
        for item in sel:
            pid = item.data(0, Qt.UserRole)
            if pid not in self.team_members.get(ti, []):
                self.team_members.setdefault(ti, []).append(pid)
        self._rebuild_team_lists()

    def _remove_from_team_specific(self, ti):
        tree = self.team_trees.get(ti)
        if not tree:
            return
        sel = tree.selectedItems()
        for item in sel:
            pid = item.data(0, Qt.UserRole)
            if pid in self.team_members.get(ti, []):
                self.team_members[ti].remove(pid)
        self._rebuild_team_lists()

    def _remove_from_team(self):
        for ti, tree in self.team_trees.items():
            sel = tree.selectedItems()
            for item in sel:
                pid = item.data(0, Qt.UserRole)
                if pid in self.team_members.get(ti, []):
                    self.team_members[ti].remove(pid)
        self._rebuild_team_lists()

    def _clear_teams(self):
        if QMessageBox.question(
                self, "Очистка", "Очистить все команды?") != QMessageBox.Yes:
            return
        for ti in self.team_members:
            self.team_members[ti] = []
        self._rebuild_team_lists()

    def _randomize_teams(self):
        sel = self.free_tree.selectedItems()
        if not sel:
            QMessageBox.information(self, "Рандомайзер",
                                    "Выберите участников из списка свободных для распределения.")
            return
        participants = [item.data(0, Qt.UserRole) for item in sel]
        random.shuffle(participants)

        for ti in self.team_members:
            self.team_members[ti] = []

        for i, pid in enumerate(participants):
            team_idx = i % len(self.team_vars)
            self.team_members.setdefault(team_idx, []).append(pid)

        self._rebuild_team_lists()
        QMessageBox.information(self, "Рандомайзер",
                                f"Успешно распределено {len(participants)} участников по командам!")

    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Матч»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_match_tab(self):
        layout = QVBoxLayout(self.tab_match)
        layout.setContentsMargins(8, 8, 8, 8)

        # Top bar
        top = QFrame()
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(12, 8, 12, 8)

        btn_new = QPushButton("Новый матч")
        btn_new.setFixedWidth(130)
        btn_new.setStyleSheet("QPushButton { font-size: 14px; font-weight: bold; }")
        btn_new.clicked.connect(self._new_match)
        top_layout.addWidget(btn_new)

        self.save_btn = QPushButton("Сохранить матч")
        self.save_btn.setFixedWidth(180)
        self.save_btn.setStyleSheet(
            "QPushButton { background-color: #98C379; color: #000000; font-size: 14px; font-weight: bold; }"
            "QPushButton:hover { background-color: #7BA862; }"
            "QPushButton:disabled { background-color: #363848; color: #555555; }")
        self.save_btn.setEnabled(False)
        self.save_btn.clicked.connect(self._manual_save)
        top_layout.addWidget(self.save_btn)

        top_layout.addStretch()

        self.match_info_lbl = QLabel("")
        self.match_info_lbl.setStyleSheet("font-size: 14px; font-weight: bold; color: #61AFEF;")
        top_layout.addWidget(self.match_info_lbl)

        layout.addWidget(top)

        # Scoreboard
        self.scoreboard = QFrame()
        self.scoreboard.setStyleSheet("QFrame { background-color: #1E2030; border-radius: 8px; }")
        self.scoreboard_layout = QHBoxLayout(self.scoreboard)
        self.scoreboard_layout.setContentsMargins(12, 6, 12, 6)
        layout.addWidget(self.scoreboard)

        # Scrollable match area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.m_scroll_widget = QWidget()
        self.m_scroll_widget.setStyleSheet("background-color: transparent;")
        self.m_scroll_layout = QVBoxLayout(self.m_scroll_widget)
        self.m_scroll_layout.setContentsMargins(0, 0, 0, 0)
        scroll.setWidget(self.m_scroll_widget)
        layout.addWidget(scroll, 1)

        # Bottom bar
        bot = QFrame()
        bot_layout = QVBoxLayout(bot)
        bot_layout.setContentsMargins(12, 8, 12, 8)

        self.score_lbl = QLabel("")
        self.score_lbl.setStyleSheet("font-size: 12px; color: #888888;")
        bot_layout.addWidget(self.score_lbl)

        self.winner_lbl = QLabel("Победитель матча:")
        self.winner_lbl.setStyleSheet("font-size: 16px; font-weight: bold; color: #888888;")
        bot_layout.addWidget(self.winner_lbl)

        layout.addWidget(bot)

    # ─── Снимок команд ───────────────────────────────────────────────────

    def _teams_snap(self):
        teams = []
        cnt = len(self.team_vars)
        for ti in range(cnt):
            members = [p for p in self.team_members.get(ti, [])
                       if p in self.persons]
            if members:
                teams.append({
                    "idx": ti,
                    "name": self.team_name(ti),
                    "color": self.team_color(ti),
                    "members": members,
                })
        return teams

    # ─── Построение матча ─────────────────────────────────────────────────

    def _new_match(self):
        self._build_match()

    def _build_match(self):
        self._vars_to_cfg()
        # Clear scroll area
        self._clear_layout(self.m_scroll_layout)
        self.round_widgets = []
        self.payload = None
        self.saved_sig = None
        self.sectors_var = False
        self.sectors_ui = None

        teams = self._teams_snap()
        if len(teams) < 2:
            self.match_info_lbl.setText("Нужно минимум 2 команды с участниками")
            self.score_lbl.setText("")
            self.winner_lbl.setText("")
            self.winner_lbl.setStyleSheet("font-size: 16px; font-weight: bold; color: #888888;")
            self._clear_layout(self.scoreboard_layout)
            return

        self.match_info_lbl.setText(f"Команд: {len(teams)}")

        # 3 rows x 3 columns grid
        for row_idx in range(3):
            # Row title
            title_lbl = QLabel(ROW_TITLES[row_idx])
            title_lbl.setAlignment(Qt.AlignCenter)
            title_lbl.setStyleSheet(
                "background-color: #FF6B35; color: #000000; font-size: 14px; "
                "font-weight: bold; border-radius: 8px; padding: 6px 12px;")
            self.m_scroll_layout.addWidget(title_lbl)

            row_widget = QWidget()
            row_widget.setStyleSheet("background-color: transparent;")
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(3, 0, 3, 0)
            row_layout.setSpacing(4)

            for col_idx in range(3):
                ri = row_idx * 3 + col_idx
                label, code = ROUND_DEFS[ri]
                cell = QWidget()
                cell.setStyleSheet("background-color: transparent;")
                cell_layout = QVBoxLayout(cell)
                cell_layout.setContentsMargins(0, 0, 0, 0)
                self._make_round(cell_layout, ri + 1, label, code, teams, ri)
                row_layout.addWidget(cell, 1)

            self.m_scroll_layout.addWidget(row_widget)

        # Sectors
        self._make_sectors_area(teams)
        self._recalc()

    # ─── Создание одного раунда ───────────────────────────────────────────

    def _make_round(self, parent_layout, rno, label, code, teams, game_idx=0):
        chess_colors = ["#3A3D4A", "#454857"]
        bg_color = chess_colors[game_idx % 2]

        # Outer: horizontal layout (main game | tiebreak)
        outer = QWidget()
        outer.setStyleSheet(f"QWidget#round_outer {{ background-color: {bg_color}; border-radius: 8px; }}")
        outer.setObjectName("round_outer")
        outer_layout = QHBoxLayout(outer)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(4)

        # Left: main game
        box = QWidget()
        box.setStyleSheet("background-color: transparent;")
        box_layout = QVBoxLayout(box)
        box_layout.setContentsMargins(10, 6, 10, 6)

        # Round header
        hdr = QLabel(f"Игра {rno}: {label}")
        hdr.setAlignment(Qt.AlignCenter)
        hdr.setStyleSheet("font-size: 13px; font-weight: bold; color: #E06C75;")
        box_layout.addWidget(hdr)

        entries = {}
        entry_widgets = {}
        total_vars = {}

        for team in teams:
            ti = team["idx"]

            # Team header + total
            team_hdr = QHBoxLayout()
            t_name = QLabel(team["name"])
            t_name.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {team['color']};")
            team_hdr.addWidget(t_name)

            sigma = QLabel("Σ")
            sigma.setStyleSheet("font-size: 11px; color: #888888;")
            team_hdr.addWidget(sigma)

            total_lbl = QLabel("0")
            total_lbl.setStyleSheet(f"font-size: 13px; font-weight: bold; color: {team['color']};")
            team_hdr.addWidget(total_lbl)
            team_hdr.addStretch()
            total_vars[ti] = total_lbl

            box_layout.addLayout(team_hdr)

            entries[ti] = {}

            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = QHBoxLayout()
                pf.setContentsMargins(12, 1, 0, 1)

                plbl = QLabel(pname)
                plbl.setStyleSheet("font-size: 12px;")
                plbl.setFixedWidth(90)
                pf.addWidget(plbl)

                re = ScoreEntry(60)
                re.textChanged.connect(lambda _: self._schedule_recalc())
                pf.addWidget(re)
                pf.addStretch()

                entries[ti][pid] = re
                entry_widgets[(ti, pid)] = re

                box_layout.addLayout(pf)

        # Diff + winner
        diff_lbl = QLabel("Разница: —")
        diff_lbl.setStyleSheet("font-size: 11px; color: #888888;")
        box_layout.addWidget(diff_lbl)

        win_lbl = QLabel("Победитель: —")
        win_lbl.setStyleSheet("font-size: 12px; font-weight: bold; color: #888888;")
        box_layout.addWidget(win_lbl)

        outer_layout.addWidget(box, 1)

        # Right: tiebreak host
        tb_host = QWidget()
        tb_host.setStyleSheet("background-color: transparent;")
        tb_host_layout = QVBoxLayout(tb_host)
        tb_host_layout.setContentsMargins(4, 0, 4, 0)
        tb_host.setVisible(False)
        outer_layout.addWidget(tb_host)

        parent_layout.addWidget(outer)

        rw = {
            "rno": rno, "label": label, "code": code, "teams": teams,
            "entries": entries, "entry_widgets": entry_widgets,
            "totals": total_vars,
            "diff_lbl": diff_lbl, "win_lbl": win_lbl,
            "tb_host": tb_host, "tb_host_layout": tb_host_layout,
            "tb_entries": None, "tb_win_lbl": None,
        }
        self.round_widgets.append(rw)

    # ─── Доп. игра (тайбрейк) ────────────────────────────────────────────

    def _handle_tiebreak(self, rw, teams, tied_indices, code):
        if rw["tb_entries"] is None:
            self._build_tiebreak(rw, teams, tied_indices, code)

        tb_entries = rw["tb_entries"]
        if tb_entries is None:
            return {}, None

        tb_scores = {}
        has_empty = False
        for ti_str, pmap in tb_entries.items():
            ti = int(ti_str)
            tb_scores[ti] = {}
            team_sum = 0
            for pid_str, entry in pmap.items():
                pid = int(pid_str)
                txt = entry.text().strip()
                if not txt:
                    has_empty = True
                    tb_scores[ti][pid] = 0
                    continue
                if is_skip(txt):
                    tb_scores[ti][pid] = SKIP
                    continue
                try:
                    v = int(txt)
                    if code != "bull" and v < 0:
                        v = 0
                    team_sum += v
                    tb_scores[ti][pid] = v
                except ValueError:
                    has_empty = True
                    tb_scores[ti][pid] = 0
            tb_scores[ti]["_sum"] = team_sum

        if has_empty:
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].setText("Доп: —")
                rw["tb_win_lbl"].setStyleSheet("font-size: 11px; color: #888888;")
            return tb_scores, None

        sums = {ti: tb_scores[ti].pop("_sum", 0) for ti in tb_scores}
        winner = self._round_winner(sums, code)
        if winner is not None:
            name = self.team_name(winner)
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].setText(f"Доп: {name}")
                rw["tb_win_lbl"].setStyleSheet(f"font-size: 11px; font-weight: bold; color: {self.team_color(winner)};")
        else:
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].setText("Доп: ничья")
                rw["tb_win_lbl"].setStyleSheet("font-size: 11px; color: #E5C07B;")
        return tb_scores, winner

    def _build_tiebreak(self, rw, teams, tied_indices, code):
        host = rw["tb_host"]
        layout = rw["tb_host_layout"]
        self._clear_layout(layout)
        host.setVisible(True)

        hdr = QLabel("Доп. игра")
        hdr.setStyleSheet("font-size: 11px; font-weight: bold; color: #E5C07B;")
        layout.addWidget(hdr)

        sub = QLabel("(по 1 дротику)")
        sub.setStyleSheet("font-size: 10px; color: #888888;")
        layout.addWidget(sub)

        tb_entries = {}
        for team in teams:
            ti = team["idx"]
            if ti not in tied_indices:
                continue

            t_lbl = QLabel(team["name"])
            t_lbl.setStyleSheet(f"font-size: 11px; font-weight: bold; color: {team['color']};")
            layout.addWidget(t_lbl)

            tb_entries[str(ti)] = {}
            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = QHBoxLayout()
                plbl = QLabel(pname)
                plbl.setStyleSheet("font-size: 10px;")
                plbl.setFixedWidth(70)
                pf.addWidget(plbl)

                ev = ScoreEntry(45)
                ev.setFixedHeight(24)
                ev.textChanged.connect(lambda _: self._schedule_recalc())
                pf.addWidget(ev)
                pf.addStretch()

                tb_entries[str(ti)][str(pid)] = ev
                layout.addLayout(pf)

        rw["tb_entries"] = tb_entries

        tb_win_lbl = QLabel("Доп: —")
        tb_win_lbl.setStyleSheet("font-size: 11px; color: #888888;")
        layout.addWidget(tb_win_lbl)
        rw["tb_win_lbl"] = tb_win_lbl

        layout.addStretch()

    def _clear_tiebreak(self, rw):
        self._clear_layout(rw["tb_host_layout"])
        rw["tb_host"].setVisible(False)
        rw["tb_entries"] = None
        rw["tb_win_lbl"] = None

    # ═══════════════════════════════════════════════════════════════════════
    #  Сектора
    # ═══════════════════════════════════════════════════════════════════════

    def _make_sectors_area(self, teams):
        wrap = QWidget()
        wrap.setStyleSheet("background-color: transparent;")
        wrap_layout = QVBoxLayout(wrap)
        wrap_layout.setContentsMargins(0, 10, 0, 0)

        # Checkbox centered
        cb_row = QHBoxLayout()
        cb_row.addStretch()
        self.sectors_cb = QCheckBox("  Сектора (2 дополнительные игры)  ")
        self.sectors_cb.setStyleSheet("font-size: 14px; font-weight: bold;")
        self.sectors_cb.setChecked(False)
        self.sectors_cb.toggled.connect(lambda checked: self._toggle_sectors(teams, checked))
        cb_row.addWidget(self.sectors_cb)
        cb_row.addStretch()
        wrap_layout.addLayout(cb_row)

        # Host for sector blocks
        self.sectors_host = QWidget()
        self.sectors_host.setStyleSheet("background-color: transparent;")
        self.sectors_host_layout = QHBoxLayout(self.sectors_host)
        self.sectors_host_layout.setContentsMargins(0, 0, 0, 0)
        self.sectors_host.setVisible(False)
        wrap_layout.addWidget(self.sectors_host)

        self.m_scroll_layout.addWidget(wrap)

    def _toggle_sectors(self, teams, checked):
        self.sectors_var = checked
        if checked:
            self._build_sectors(teams)
        else:
            self._clear_layout(self.sectors_host_layout)
            self.sectors_host.setVisible(False)
            self.sectors_ui = None
            self._schedule_recalc()

    def _build_one_sector_box(self, parent_layout, game_no, teams):
        box = QFrame()
        box.setStyleSheet("QFrame { background-color: #2B2D3A; border-radius: 10px; }")
        box_layout = QVBoxLayout(box)
        box_layout.setContentsMargins(16, 10, 16, 10)

        hdr = QLabel(f"Сектора {game_no}")
        hdr.setAlignment(Qt.AlignCenter)
        hdr.setStyleSheet("font-size: 15px; font-weight: bold; color: #61AFEF;")
        box_layout.addWidget(hdr)

        # Sector fields
        sf = QHBoxLayout()
        sf.addWidget(QLabel("Секторы:"))
        sec_entries = []
        for i in range(3):
            se = QLineEdit()
            se.setFixedWidth(60)
            se.setFixedHeight(28)
            se.setPlaceholderText(f"С{i+1}")
            sf.addWidget(se)
            sec_entries.append(se)
        sf.addStretch()
        box_layout.addLayout(sf)

        # Teams
        entries = {}
        total_vars = {}

        teams_row = QHBoxLayout()
        for team in teams:
            ti = team["idx"]
            tf = QFrame()
            tf.setStyleSheet("QFrame { background-color: #363848; border-radius: 8px; }")
            tf_layout = QVBoxLayout(tf)
            tf_layout.setContentsMargins(10, 8, 10, 8)

            t_lbl = QLabel(team["name"])
            t_lbl.setStyleSheet(f"font-size: 13px; font-weight: bold; color: {team['color']};")
            tf_layout.addWidget(t_lbl)

            entries[ti] = {}
            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = QHBoxLayout()
                plbl = QLabel(pname)
                plbl.setStyleSheet("font-size: 12px;")
                plbl.setFixedWidth(80)
                pf.addWidget(plbl)
                ev = ScoreEntry(50)
                ev.setFixedHeight(26)
                ev.textChanged.connect(lambda _: self._schedule_recalc())
                pf.addWidget(ev)
                pf.addStretch()
                entries[ti][pid] = ev
                tf_layout.addLayout(pf)

            # Total
            tot_row = QHBoxLayout()
            tot_row.addWidget(QLabel("Итого:"))
            tv = QLabel("0")
            tv.setStyleSheet(f"font-size: 13px; font-weight: bold; color: {team['color']};")
            tot_row.addWidget(tv)
            tot_row.addStretch()
            total_vars[ti] = tv
            tf_layout.addLayout(tot_row)

            teams_row.addWidget(tf)

        box_layout.addLayout(teams_row)

        # Winner
        sw_lbl = QLabel("Победитель: —")
        sw_lbl.setAlignment(Qt.AlignCenter)
        sw_lbl.setStyleSheet("font-size: 14px; font-weight: bold; color: #888888;")
        box_layout.addWidget(sw_lbl)

        parent_layout.addWidget(box)

        return {
            "teams": teams,
            "sec_entries": sec_entries,
            "entries": entries,
            "total_vars": total_vars,
            "win_lbl": sw_lbl,
        }

    def _build_sectors(self, teams):
        self._clear_layout(self.sectors_host_layout)
        self.sectors_host.setVisible(True)

        self.sectors_ui = []
        for g in range(2):
            ui = self._build_one_sector_box(self.sectors_host_layout, g + 1, teams)
            self.sectors_ui.append(ui)

        self._schedule_recalc()

    def _calc_one_sector(self, ui):
        if not ui:
            return None, None

        totals = {}
        any_filled = False

        for team in ui["teams"]:
            ti = team["idx"]
            team_total = 0
            for pid, entry in ui["entries"][ti].items():
                txt = entry.text().strip()
                if txt and not is_skip(txt):
                    try:
                        v = int(txt)
                        if v < 0:
                            v = 0
                        team_total += v
                        any_filled = True
                    except ValueError:
                        pass
            totals[ti] = team_total
            ui["total_vars"][ti].setText(str(team_total))

        if not any_filled:
            ui["win_lbl"].setText("Победитель: —")
            ui["win_lbl"].setStyleSheet("font-size: 14px; font-weight: bold; color: #888888;")
            return None, None

        max_val = max(totals.values())
        leaders = [ti for ti, v in totals.items() if v == max_val]

        scores_data = {}
        for team in ui["teams"]:
            ti = team["idx"]
            scores_data[str(ti)] = {}
            for pid, entry in ui["entries"][ti].items():
                txt = entry.text().strip()
                if is_skip(txt):
                    scores_data[str(ti)][str(pid)] = SKIP
                elif txt:
                    try:
                        scores_data[str(ti)][str(pid)] = max(0, int(txt))
                    except ValueError:
                        scores_data[str(ti)][str(pid)] = 0
                else:
                    scores_data[str(ti)][str(pid)] = 0

        sec_names = [se.text().strip() for se in ui["sec_entries"]]

        if len(leaders) == 1:
            w_idx = leaders[0]
            name = self.team_name(w_idx)
            color = self.team_color(w_idx)
            ui["win_lbl"].setText(f"Победитель: {name}")
            ui["win_lbl"].setStyleSheet(f"font-size: 14px; font-weight: bold; color: {color};")
            data = {"totals": totals, "scores": scores_data,
                    "sectors": sec_names, "winner": w_idx}
            return data, w_idx
        else:
            ui["win_lbl"].setText("Победитель: ничья")
            ui["win_lbl"].setStyleSheet("font-size: 14px; font-weight: bold; color: #E5C07B;")
            data = {"totals": totals, "scores": scores_data,
                    "sectors": sec_names, "winner": None}
            return data, None

    def _calc_sectors(self):
        if not self.sectors_ui:
            return None, []
        all_data = []
        winners = []
        for ui in self.sectors_ui:
            data, w = self._calc_one_sector(ui)
            all_data.append(data)
            winners.append(w)
        return all_data, winners

    # ═══════════════════════════════════════════════════════════════════════
    #  Пересчёт матча
    # ═══════════════════════════════════════════════════════════════════════

    def _recalc(self):
        teams = self._teams_snap()
        if len(teams) < 2:
            return

        wins = {t["idx"]: 0 for t in teams}
        raw_totals = {t["idx"]: 0 for t in teams}
        all_filled = True
        all_decided = True
        rounds_data = []

        for rw in self.round_widgets:
            code = rw["code"]
            totals = {}
            scores = {}
            has_empty = False

            for team in teams:
                ti = team["idx"]
                team_sum = 0
                scores[ti] = {}

                for pid in team["members"]:
                    entry = rw["entries"].get(ti, {}).get(pid)
                    if entry is None:
                        has_empty = True
                        scores[ti][pid] = 0
                        continue
                    txt = entry.text().strip()
                    if not txt:
                        has_empty = True
                        scores[ti][pid] = 0
                        continue
                    if is_skip(txt):
                        scores[ti][pid] = SKIP
                        continue
                    try:
                        v = int(txt)
                        if code != "bull" and v < 0:
                            v = 0
                        team_sum += v
                        scores[ti][pid] = v
                    except ValueError:
                        has_empty = True
                        scores[ti][pid] = 0

                totals[ti] = team_sum
                raw_totals[ti] = raw_totals.get(ti, 0) + team_sum

            if has_empty:
                all_filled = False

            # Update totals
            for ti, lbl in rw["totals"].items():
                lbl.setText(str(totals.get(ti, 0)))

            # Determine round winner
            winner = self._round_winner(totals, code)
            diff = self._round_diff(totals)
            rw["diff_lbl"].setText(f"Разница: {diff}" if diff is not None else "Разница: —")

            # Tiebreak
            tb_scores = {}
            final_winner = winner
            if winner is None and not has_empty and len(totals) >= 2:
                tied = self._tied_teams(totals, code)
                if len(tied) >= 2:
                    tb_scores, tb_winner = self._handle_tiebreak(rw, teams, tied, code)
                    if tb_winner is not None:
                        final_winner = tb_winner
                    else:
                        all_decided = False
                else:
                    self._clear_tiebreak(rw)
            else:
                self._clear_tiebreak(rw)

            if final_winner is not None:
                wins[final_winner] += 1
                name = self.team_name(final_winner)
                color = self.team_color(final_winner)
                rw["win_lbl"].setText(f"Победитель: {name}")
                rw["win_lbl"].setStyleSheet(f"font-size: 12px; font-weight: bold; color: {color};")
            elif has_empty:
                rw["win_lbl"].setText("Победитель: —")
                rw["win_lbl"].setStyleSheet("font-size: 12px; font-weight: bold; color: #888888;")
                all_decided = False
            else:
                rw["win_lbl"].setText("Победитель: ничья")
                rw["win_lbl"].setStyleSheet("font-size: 12px; font-weight: bold; color: #E5C07B;")
                all_decided = False

            rounds_data.append({
                "rno": rw["rno"], "label": rw["label"], "mode": code,
                "totals": totals,
                "scores": {str(ti): {str(p): v for p, v in sm.items()}
                           for ti, sm in scores.items()},
                "winner": final_winner,
                "tb_scores": {str(ti): {str(p): v for p, v in sm.items()}
                              for ti, sm in tb_scores.items()}
                             if tb_scores else {},
            })

        # --- Sectors ---
        sectors_data = None
        sectors_winners = []
        if self.sectors_var and self.sectors_ui:
            sectors_data, sectors_winners = self._calc_sectors()
            for sw in sectors_winners:
                if sw is not None:
                    wins[sw] += 1

        # --- Scoreboard ---
        self._draw_scoreboard(teams, wins)

        # --- Standings ---
        standings = sorted(wins.items(), key=lambda x: x[1], reverse=True)
        score_parts = [f"{self.team_name(ti)} {w}" for ti, w in standings]
        raw_parts = [f"{self.team_name(ti)} {raw_totals.get(ti, 0)}"
                     for ti, _ in standings]
        self.score_lbl.setText(
            f"Победы: {' | '.join(score_parts)}   "
            f"Сумма: {' | '.join(raw_parts)}")

        # --- Winner ---
        leaders = ([ti for ti, w in standings if w == standings[0][1]]
                   if standings else [])

        undecided = sum(
            1 for rw in self.round_widgets
            if rw["win_lbl"].text().endswith("ничья")
            or rw["win_lbl"].text().endswith("—"))

        if self.sectors_var:
            for sw in sectors_winners:
                if sw is None:
                    undecided += 1

        if standings and len(standings) >= 2:
            second_max = standings[1][1] + undecided
            leader_wins = standings[0][1]
            insurmountable = leader_wins > second_max and len(leaders) == 1
        else:
            insurmountable = False

        complete = (
            (all_filled and all_decided and len(leaders) == 1)
            or (all_filled and insurmountable)
        )

        if (self.sectors_var
                and any(sw is None for sw in sectors_winners)
                and not insurmountable):
            complete = False

        if complete:
            w_idx = leaders[0]
            w_name = self.team_name(w_idx)
            w_color = self.team_color(w_idx)
            parts = ["9 игр"]
            n_sector_wins = sum(1 for sw in sectors_winners if sw is not None)
            if n_sector_wins > 0:
                parts.append(f"сектора ({n_sector_wins})")
            reason = " + ".join(parts)
        else:
            w_name = ""
            w_color = "#888888"
            reason = ""

        self.winner_lbl.setText(
            f"Победитель матча: {w_name}" if w_name
            else "Победитель матча:")
        self.winner_lbl.setStyleSheet(
            f"font-size: 16px; font-weight: bold; color: {w_color};")

        # Payload
        self.payload = {
            "teams": [{"idx": t["idx"], "name": t["name"],
                        "color": t["color"], "members": t["members"]}
                       for t in teams],
            "rounds": rounds_data,
            "wins": wins,
            "raw_totals": raw_totals,
            "winner": w_name if complete else None,
            "reason": reason,
            "summary": f"Победы: {' | '.join(score_parts)}",
            "sectors": sectors_data,
        }

        self.save_btn.setEnabled(complete)

    # ─── Логика раунда ────────────────────────────────────────────────────

    def _round_winner(self, totals, code):
        if len(totals) < 2:
            return None
        items = list(totals.items())
        if code == "min":
            best = min(items, key=lambda x: x[1])
            if sum(1 for _, v in items if v == best[1]) == 1:
                return best[0]
        else:
            best = max(items, key=lambda x: x[1])
            if sum(1 for _, v in items if v == best[1]) == 1:
                return best[0]
        return None

    def _round_diff(self, totals):
        vals = list(totals.values())
        if len(vals) >= 2:
            return abs(max(vals) - min(vals))
        return None

    def _tied_teams(self, totals, code):
        items = list(totals.items())
        if code == "min":
            best = min(v for _, v in items)
        else:
            best = max(v for _, v in items)
        return [ti for ti, v in items if v == best]

    # ─── Табло ────────────────────────────────────────────────────────────

    def _draw_scoreboard(self, teams, wins):
        self._clear_layout(self.scoreboard_layout)
        if not teams:
            return

        self.scoreboard_layout.addStretch()
        for i, team in enumerate(teams):
            ti = team["idx"]
            w = wins.get(ti, 0)
            if i > 0:
                dash = QLabel("—")
                dash.setStyleSheet("font-size: 24px; font-weight: bold; color: #555555;")
                self.scoreboard_layout.addWidget(dash)

            tf = QVBoxLayout()
            t_name = QLabel(team["name"])
            t_name.setAlignment(Qt.AlignCenter)
            t_name.setStyleSheet(f"font-size: 12px; color: {team['color']};")
            tf.addWidget(t_name)

            t_score = QLabel(str(w))
            t_score.setAlignment(Qt.AlignCenter)
            t_score.setStyleSheet(f"font-size: 32px; font-weight: bold; color: {team['color']};")
            tf.addWidget(t_score)

            container = QWidget()
            container.setLayout(tf)
            container.setStyleSheet("background-color: transparent;")
            self.scoreboard_layout.addWidget(container)

        self.scoreboard_layout.addStretch()

    # ═══════════════════════════════════════════════════════════════════════
    #  Сохранение
    # ═══════════════════════════════════════════════════════════════════════

    def _manual_save(self):
        if not self.payload or not self.payload.get("winner"):
            QMessageBox.warning(self, "Сохранение", "Матч ещё не завершён.")
            return
        sig = json.dumps(self.payload, sort_keys=True, ensure_ascii=False)
        if sig == self.saved_sig:
            QMessageBox.information(self, "Сохранение", "Этот результат уже сохранён.")
            return
        w = self.payload["winner"]
        if QMessageBox.question(
                self, "Сохранение",
                f"Сохранить матч?\nПобедитель: {w}") == QMessageBox.Yes:
            self.db.save_match(self.payload)
            self.saved_sig = sig
            self._refresh_history()
            QMessageBox.information(self, "Готово", "Матч сохранён!")

    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Статистика»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_stats_tab(self):
        layout = QVBoxLayout(self.tab_stats)
        layout.setContentsMargins(8, 8, 8, 8)

        # --- Statistics ---
        st_frame = QFrame()
        st_layout = QVBoxLayout(st_frame)
        st_layout.setContentsMargins(8, 8, 8, 8)

        lbl = QLabel("Статистика игроков")
        lbl.setStyleSheet("font-size: 14px; font-weight: bold;")
        st_layout.addWidget(lbl)

        self.st_tree = QTreeWidget()
        self.st_tree.setHeaderLabels([
            "Имя", "Матчей", "Ср. макс", "Ранг", "Ср. мин", "Ранг",
            "Ср. бул", "Ранг", "Ср. место", "Общий"
        ])
        self.st_tree.setAlternatingRowColors(True)
        self.st_tree.setRootIsDecorated(False)
        self.st_tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.st_tree.setSortingEnabled(False)
        # Column widths
        widths = [130, 60, 80, 50, 80, 50, 80, 50, 80, 60]
        for i, w in enumerate(widths):
            self.st_tree.setColumnWidth(i, w)
        self.st_tree.header().sectionClicked.connect(self._on_stats_header_click)
        self.st_tree.itemDoubleClicked.connect(self._show_player_graph)

        st_layout.addWidget(self.st_tree)
        layout.addWidget(st_frame, 1)

        # --- History ---
        hi_frame = QFrame()
        hi_layout = QVBoxLayout(hi_frame)
        hi_layout.setContentsMargins(8, 8, 8, 8)

        hi_top = QHBoxLayout()

        lbl2 = QLabel("История матчей")
        lbl2.setStyleSheet("font-size: 14px; font-weight: bold;")
        hi_top.addWidget(lbl2)

        hi_top.addSpacing(30)

        hi_top.addWidget(QLabel("С:"))
        self.filter_start = QLineEdit()
        self.filter_start.setFixedWidth(100)
        self.filter_start.setPlaceholderText("YYYY-MM-DD")
        hi_top.addWidget(self.filter_start)

        hi_top.addWidget(QLabel("По:"))
        self.filter_end = QLineEdit()
        self.filter_end.setFixedWidth(100)
        self.filter_end.setPlaceholderText("YYYY-MM-DD")
        hi_top.addWidget(self.filter_end)

        btn_filter = QPushButton("Фильтр")
        btn_filter.setFixedWidth(80)
        btn_filter.clicked.connect(self._filter_history)
        hi_top.addWidget(btn_filter)

        self.filter_count_lbl = QLabel("")
        self.filter_count_lbl.setStyleSheet("font-size: 11px; color: #888888;")
        hi_top.addWidget(self.filter_count_lbl)

        hi_top.addStretch()

        btn_show = QPushButton("Показать")
        btn_show.setFixedWidth(100)
        btn_show.clicked.connect(self._show_match)
        hi_top.addWidget(btn_show)

        btn_del = QPushButton("Удалить выбранные")
        btn_del.setFixedWidth(160)
        btn_del.setStyleSheet("QPushButton { background-color: #E06C75; } QPushButton:hover { background-color: #C85A63; }")
        btn_del.clicked.connect(self._delete_matches)
        hi_top.addWidget(btn_del)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.setFixedWidth(100)
        btn_refresh.clicked.connect(self._refresh_history)
        hi_top.addWidget(btn_refresh)

        hi_layout.addLayout(hi_top)

        self.hi_tree = QTreeWidget()
        self.hi_tree.setHeaderLabels(["Дата", "Результат", "Победитель", "Определение"])
        self.hi_tree.setColumnWidth(0, 150)
        self.hi_tree.setColumnWidth(1, 500)
        self.hi_tree.setColumnWidth(2, 150)
        self.hi_tree.setColumnWidth(3, 200)
        self.hi_tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.hi_tree.setAlternatingRowColors(True)
        self.hi_tree.setRootIsDecorated(False)
        self.hi_tree.itemDoubleClicked.connect(self._show_match)

        hi_layout.addWidget(self.hi_tree)
        layout.addWidget(hi_frame, 1)

    def _on_stats_header_click(self, index):
        """Sort by 'Общий' column (index 9)."""
        if index == 9:
            self._sort_by_overall()

    def _sort_by_overall(self):
        self.st_tree.clear()
        stats = self.db.calc_stats()
        sorted_stats = sorted(stats, key=lambda x: x.get("overall") or 99999)
        for r in sorted_stats:
            item = QTreeWidgetItem([
                r["name"], str(r["n"]),
                fmt(r["avg_max"]), fmt(r["r_max"]),
                fmt(r["avg_min"]), fmt(r["r_min"]),
                fmt(r["avg_bull"]), fmt(r["r_bull"]),
                fmt(r["avg_p"]), fmt(r["overall"]),
            ])
            self.st_tree.addTopLevelItem(item)

    # ─── Обновление статистики и истории ──────────────────────────────────

    def _refresh_history(self):
        # Stats
        self.st_tree.clear()
        for r in self.db.calc_stats():
            item = QTreeWidgetItem([
                r["name"], str(r["n"]),
                fmt(r["avg_max"]), fmt(r["r_max"]),
                fmt(r["avg_min"]), fmt(r["r_min"]),
                fmt(r["avg_bull"]), fmt(r["r_bull"]),
                fmt(r["avg_p"]), fmt(r["overall"]),
            ])
            self.st_tree.addTopLevelItem(item)

        # History
        self.hi_tree.clear()
        for row in self.db.all_matches():
            item = QTreeWidgetItem([
                row["played_at"] or "",
                row["summary"] or "",
                row["winner"] or "—",
                row["reason"] or "",
            ])
            item.setData(0, Qt.UserRole, row["id"])
            self.hi_tree.addTopLevelItem(item)
        self.filter_count_lbl.setText("")

    def _filter_history(self):
        start_date = self.filter_start.text().strip()
        end_date = self.filter_end.text().strip()

        all_matches = self.db.all_matches()

        if start_date and end_date:
            filtered = []
            for m in all_matches:
                played_at = m.get("played_at", "")
                if played_at:
                    date_part = played_at.split()[0] if " " in played_at else played_at
                    if start_date <= date_part <= end_date:
                        filtered.append(m)
        elif start_date:
            filtered = [m for m in all_matches if m.get("played_at", "") and
                        m.get("played_at", "").split()[0] >= start_date]
        elif end_date:
            filtered = [m for m in all_matches if m.get("played_at", "") and
                        m.get("played_at", "").split()[0] <= end_date]
        else:
            filtered = all_matches

        self.hi_tree.clear()
        for row in filtered:
            item = QTreeWidgetItem([
                row["played_at"] or "",
                row["summary"] or "",
                row["winner"] or "—",
                row["reason"] or "",
            ])
            item.setData(0, Qt.UserRole, row["id"])
            self.hi_tree.addTopLevelItem(item)

        count = len(filtered)
        total = len(all_matches)
        self.filter_count_lbl.setText(f"Показано: {count} из {total}")

    # ─── Просмотр матча ──────────────────────────────────────────────────

    def _show_match(self, item=None):
        sel = self.hi_tree.selectedItems()
        if not sel:
            return
        mid = sel[0].data(0, Qt.UserRole)
        if mid is None:
            return
        row = self.db.match_by_id(mid)
        if not row:
            return
        try:
            p = json.loads(row["payload"])
        except Exception:
            QMessageBox.critical(self, "Ошибка",
                                  "Не удалось прочитать данные матча.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Матч #{mid} — {row['played_at']}")
        dlg.resize(800, 700)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        content.setStyleSheet("background-color: #24273A;")
        cl = QVBoxLayout(content)
        cl.setContentsMargins(12, 12, 12, 12)

        hdr = QLabel(f"Матч #{mid}")
        hdr.setStyleSheet("font-size: 20px; font-weight: bold; color: #61AFEF;")
        cl.addWidget(hdr)

        cl.addWidget(QLabel(f"Дата: {row['played_at']}"))

        win_lbl = QLabel(f"Победитель: {row['winner'] or '—'}")
        win_lbl.setStyleSheet("font-size: 16px; font-weight: bold; color: #98C379;")
        cl.addWidget(win_lbl)

        cl.addWidget(QLabel(f"Определение: {row['reason'] or '—'}"))

        team_map = {}
        for t in p.get("teams", []):
            team_map[str(t["idx"])] = t

        for rnd in p.get("rounds", []):
            rf = QFrame()
            rf.setStyleSheet("QFrame { background-color: #2B2D3A; border-radius: 8px; }")
            rf_layout = QVBoxLayout(rf)
            rf_layout.setContentsMargins(10, 6, 10, 6)

            r_hdr = QLabel(f"Игра {rnd['rno']}: {rnd['label']}")
            r_hdr.setStyleSheet("font-size: 13px; font-weight: bold; color: #E06C75;")
            rf_layout.addWidget(r_hdr)

            winner = rnd.get("winner")
            for ti_str, smap in rnd.get("scores", {}).items():
                team = team_map.get(ti_str, {})
                total = rnd.get("totals", {}).get(int(ti_str), 0)
                t_lbl = QLabel(f"{team.get('name', '?')} — Σ {total}")
                t_lbl.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {team.get('color', '#CDD6F4')};")
                rf_layout.addWidget(t_lbl)

                for pid_s, val in smap.items():
                    pname = self.persons.get(int(pid_s), pid_s)
                    disp = ("x (пропуск)" if val == SKIP or val == "x"
                            else str(val))
                    rf_layout.addWidget(QLabel(f"    {pname}: {disp}"))

            if winner is not None:
                wt = team_map.get(str(winner), {})
                wl = QLabel(f"Победитель: {wt.get('name', '?')}")
                wl.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {wt.get('color', '#CDD6F4')};")
                rf_layout.addWidget(wl)
            else:
                wl = QLabel("Победитель: ничья")
                wl.setStyleSheet("font-size: 12px; color: #E5C07B;")
                rf_layout.addWidget(wl)

            # Tiebreak
            tb = rnd.get("tb_scores", {})
            if tb:
                tb_hdr = QLabel("Доп. игра (по 1 дротику):")
                tb_hdr.setStyleSheet("font-size: 12px; font-weight: bold; color: #E5C07B;")
                rf_layout.addWidget(tb_hdr)
                for ti_str, smap in tb.items():
                    team = team_map.get(ti_str, {})
                    for pid_s, val in smap.items():
                        pname = self.persons.get(int(pid_s), pid_s)
                        disp = ("x (пропуск)" if val == SKIP or val == "x"
                                else str(val))
                        rf_layout.addWidget(
                            QLabel(f"    {team.get('name', '?')} · {pname}: {disp}"))

            cl.addWidget(rf)

        # Sectors data
        sectors = p.get("sectors")
        if sectors:
            for i, sd in enumerate(sectors):
                if sd is None:
                    continue
                sf = QFrame()
                sf.setStyleSheet("QFrame { background-color: #2B2D3A; border-radius: 8px; }")
                sf_layout = QVBoxLayout(sf)
                sf_layout.setContentsMargins(10, 6, 10, 6)

                s_hdr = QLabel(f"Сектора {i+1}")
                s_hdr.setStyleSheet("font-size: 13px; font-weight: bold; color: #61AFEF;")
                sf_layout.addWidget(s_hdr)

                sec_names = sd.get("sectors", [])
                if sec_names:
                    sf_layout.addWidget(QLabel(f"Секторы: {', '.join(sec_names)}"))

                for ti_str, smap in sd.get("scores", {}).items():
                    team = team_map.get(ti_str, {})
                    total = sd.get("totals", {}).get(int(ti_str), 0)
                    t_lbl = QLabel(f"{team.get('name', '?')} — Σ {total}")
                    t_lbl.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {team.get('color', '#CDD6F4')};")
                    sf_layout.addWidget(t_lbl)
                    for pid_s, val in smap.items():
                        pname = self.persons.get(int(pid_s), pid_s)
                        disp = ("x (пропуск)" if val == SKIP or val == "x" else str(val))
                        sf_layout.addWidget(QLabel(f"    {pname}: {disp}"))

                sw = sd.get("winner")
                if sw is not None:
                    wt = team_map.get(str(sw), {})
                    wl = QLabel(f"Победитель: {wt.get('name', '?')}")
                    wl.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {wt.get('color', '#CDD6F4')};")
                    sf_layout.addWidget(wl)

                cl.addWidget(sf)

        summary = QLabel(p.get("summary", ""))
        summary.setStyleSheet("font-size: 12px; color: #888888;")
        cl.addWidget(summary)
        cl.addStretch()

        scroll.setWidget(content)
        dlg_layout = QVBoxLayout(dlg)
        dlg_layout.addWidget(scroll)
        dlg.exec()

    def _delete_matches(self):
        sel = self.hi_tree.selectedItems()
        if not sel:
            QMessageBox.information(
                self, "Удаление",
                "Выберите матчи для удаления.\n"
                "Используйте Ctrl+клик для множественного выбора.")
            return
        n = len(sel)
        word = "матч" if n == 1 else f"матчей: {n}"
        if QMessageBox.question(
                self, "Удаление", f"Удалить {word}?") != QMessageBox.Yes:
            return
        ids = [item.data(0, Qt.UserRole) for item in sel]
        self.db.delete_matches(ids)
        self._refresh_history()

    # ─── График игрока ───────────────────────────────────────────────────

    def _show_player_graph(self, item=None):
        if not HAS_MATPLOTLIB:
            QMessageBox.critical(self, "Ошибка",
                                  "Для отображения графика установите matplotlib:\npip install matplotlib")
            return

        sel = self.st_tree.selectedItems()
        if not sel:
            return

        player_name = sel[0].text(0)

        pid = None
        for p_id, name in self.persons.items():
            if name == player_name:
                pid = p_id
                break

        if pid is None:
            return

        history = self.db.get_player_history(pid)
        if not history:
            QMessageBox.information(self, "График", "Нет данных для отображения.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"График игрока: {player_name}")
        dlg.resize(900, 600)

        dlg_layout = QVBoxLayout(dlg)

        match_nums = list(range(1, len(history) + 1))
        max_vals = [h["avg_max"] for h in history]
        min_vals = [h["avg_min"] for h in history]
        bull_vals = [h["avg_bull"] for h in history]

        fig = Figure(figsize=(10, 6), facecolor="#2B2D3A")
        ax = fig.add_subplot(111)
        ax.set_facecolor("#2B2D3A")

        if any(v is not None for v in max_vals):
            ax.plot(match_nums,
                    [v if v is not None else 0 for v in max_vals],
                    marker='o', color='#98C379',
                    label='Среднее (Больше)', linewidth=2)
        if any(v is not None for v in min_vals):
            ax.plot(match_nums,
                    [v if v is not None else 0 for v in min_vals],
                    marker='s', color='#E5C07B',
                    label='Среднее (Меньше)', linewidth=2)
        if any(v is not None for v in bull_vals):
            ax.plot(match_nums,
                    [v if v is not None else 0 for v in bull_vals],
                    marker='^', color='#C678DD',
                    label='Среднее (Булл)', linewidth=2)

        ax.set_xlabel('Номер матча', color='#CDD6F4', fontsize=12)
        ax.set_ylabel('Средний результат', color='#CDD6F4', fontsize=12)
        ax.set_title(f'Динамика результатов: {player_name}',
                     color='#CDD6F4', fontsize=14)

        ax.tick_params(colors='#CDD6F4')
        ax.spines['bottom'].set_color('#CDD6F4')
        ax.spines['left'].set_color('#CDD6F4')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        ax.legend(loc='upper right', facecolor='#363848',
                  edgecolor='#CDD6F4', labelcolor='#CDD6F4')
        ax.grid(True, alpha=0.3, color='#CDD6F4')

        canvas = FigureCanvas(fig)
        dlg_layout.addWidget(canvas)
        dlg.exec()

    # ─── Выход ────────────────────────────────────────────────────────────

    def closeEvent(self, event):
        self._vars_to_cfg()
        self._save_members()
        event.accept()


# ═══════════════════════════════════════════════════════════════════════════════
#  Запуск
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(DARK_QSS)
    window = App()
    window.show()
    sys.exit(app.exec())
