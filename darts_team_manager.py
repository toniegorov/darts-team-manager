#!/usr/bin/env python3
"""
Дартс · Командный менеджер  (CustomTkinter edition)
====================================================
Правила:
  - 9 основных игр: 3 на максимум, 3 на минимум, 3 на бул
  - Бул: значения от -25 до 150, просто суммируются
  - При ничьей — «Доп. игра (по 1 дротику)»
  - Галочка «вручную» → 3 поля для дротиков → OK → сумма
  - Ввод «x» = пропуск (не считается в сумму и статистику)
  - Отрицательные значения разрешены только в бул
  - Сектора: дополнительный раунд, +1 к общему счёту
  - Ручное сохранение (кнопка), множественное удаление истории

Зависимости:  pip install customtkinter
"""

import json
import sqlite3
import os
import sys
from datetime import datetime
from statistics import mean
from collections import defaultdict
from typing import Optional, Any

try:
    import customtkinter as ctk
except ImportError:
    print("Установите customtkinter: pip install customtkinter")
    sys.exit(1)

import tkinter as tk
from tkinter import colorchooser, messagebox
from tkinter import ttk  # для Treeview (нет аналога в CTk)

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

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "darts_manager.db")


# ═══════════════════════════════════════════════════════════════════════════════
#  Утилиты
# ═══════════════════════════════════════════════════════════════════════════════

def is_skip(txt: str) -> bool:
    return txt.strip().lower() == "x"


def safe_mean(lst):
    vals = [v for v in lst if v is not None and v != SKIP]
    return round(mean(vals), 1) if vals else None


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
#  База данных
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

        # Ранги
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Главное приложение (CustomTkinter)
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):

    def __init__(self):
        super().__init__()

        # Тема
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.title("Дартс · Командный менеджер")
        self.geometry("1500x920")
        self.minsize(1200, 700)

        self.db = DB()
        self.persons = {}
        self.team_members = {}
        self.round_widgets = []
        self.sectors_var = None
        self.sectors_ui = None
        self.payload = None
        self.saved_sig = None
        self._recalc_id = None

        # Конфигурация команд
        self.team_cfg = self.db.load_state("team_cfg", {
            "count": 2,
            "teams": [{"name": "Команда 1", "color": PALETTE[0]},
                      {"name": "Команда 2", "color": PALETTE[1]}],
        })
        self._ensure_cfg()

        self.team_count_var = tk.StringVar(value=str(self.team_cfg["count"]))
        self.team_vars = []
        self._cfg_to_vars()

        self.save_teams_var = tk.BooleanVar(
            value=self.db.load_state("save_teams", False))

        self.protocol("WM_DELETE_WINDOW", self._quit)
        self._build()
        self._refresh_persons()
        if self.save_teams_var.get():
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

    def _cfg_to_vars(self):
        self.team_vars = []
        for t in self.team_cfg["teams"]:
            self.team_vars.append({
                "name": tk.StringVar(value=t["name"]),
                "color": tk.StringVar(value=t["color"]),
            })

    def _vars_to_cfg(self):
        teams = []
        for tv in self.team_vars:
            teams.append({"name": tv["name"].get(), "color": tv["color"].get()})
        self.team_cfg = {"count": len(teams), "teams": teams}
        self.db.save_state("team_cfg", self.team_cfg)

    def _save_members(self):
        if self.save_teams_var.get():
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
            return self.team_vars[ti]["name"].get()
        return f"Команда {ti+1}"

    def team_color(self, ti):
        if 0 <= ti < len(self.team_vars):
            return self.team_vars[ti]["color"].get()
        return "#888888"

    # ─── Построение основного UI ──────────────────────────────────────────

    def _build(self):
        self.nb = ctk.CTkTabview(self, corner_radius=12)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)

        self.nb.add("  👥 Участники  ")
        self.nb.add("  🎯 Матч  ")
        self.nb.add("  📊 Статистика  ")

        self.tab_people = self.nb.tab("  👥 Участники  ")
        self.tab_match = self.nb.tab("  🎯 Матч  ")
        self.tab_stats = self.nb.tab("  📊 Статистика  ")

        self._build_people_tab()
        self._build_match_tab()
        self._build_stats_tab()

    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Участники»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_people_tab(self):
        root = self.tab_people

        # --- Настройки команд ---
        cfg_frame = ctk.CTkFrame(root, corner_radius=10)
        cfg_frame.pack(fill="x", padx=8, pady=(8, 4))

        cfg_top = ctk.CTkFrame(cfg_frame, fg_color="transparent")
        cfg_top.pack(fill="x", padx=12, pady=(10, 6))

        ctk.CTkLabel(cfg_top, text="Количество команд:",
                     font=("Segoe UI", 14)).pack(side="left")

        self.count_spin = ctk.CTkOptionMenu(
            cfg_top, values=["2", "3", "4", "5"],
            variable=self.team_count_var,
            command=lambda _: self._on_count_change(),
            width=70, corner_radius=8)
        self.count_spin.pack(side="left", padx=(8, 20))

        ctk.CTkCheckBox(cfg_top, text="Сохранять составы",
                        variable=self.save_teams_var,
                        command=self._save_members,
                        corner_radius=6).pack(side="left")

        self.cfg_host = ctk.CTkFrame(cfg_frame, fg_color="transparent")
        self.cfg_host.pack(fill="x", padx=12, pady=(0, 10))
        self._rebuild_cfg_row()

        # --- Тело: свободные + команды ---
        body = ctk.CTkFrame(root, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=8, pady=4)
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=3)
        body.rowconfigure(0, weight=1)

        # Свободные участники
        free_frame = ctk.CTkFrame(body, corner_radius=10)
        free_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))

        ctk.CTkLabel(free_frame, text="Свободные участники",
                     font=("Segoe UI", 14, "bold")).pack(
            anchor="w", padx=12, pady=(10, 4))

        tree_frame = ctk.CTkFrame(free_frame, fg_color="transparent")
        tree_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        # Стилизация Treeview под тёмную тему
        self._style_treeview()

        self.free_tree = ttk.Treeview(tree_frame, columns=("id", "name"),
                                      show="headings", selectmode="extended",
                                      style="Dark.Treeview")
        self.free_tree.heading("id", text="ID")
        self.free_tree.heading("name", text="Имя")
        self.free_tree.column("id", width=40, anchor="center")
        self.free_tree.column("name", width=150, anchor="w")
        fsb = ctk.CTkScrollbar(tree_frame, command=self.free_tree.yview)
        self.free_tree.configure(yscrollcommand=fsb.set)
        self.free_tree.pack(side="left", fill="both", expand=True)
        fsb.pack(side="right", fill="y")

        # Составы команд
        teams_frame = ctk.CTkFrame(body, corner_radius=10)
        teams_frame.grid(row=0, column=1, sticky="nsew")

        ctk.CTkLabel(teams_frame, text="Составы команд",
                     font=("Segoe UI", 14, "bold")).pack(
            anchor="w", padx=12, pady=(10, 4))

        self.teams_host = ctk.CTkFrame(teams_frame, fg_color="transparent")
        self.teams_host.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        # --- Панель действий ---
        bar = ctk.CTkFrame(root, corner_radius=10)
        bar.pack(fill="x", padx=8, pady=(4, 8))

        bar_inner = ctk.CTkFrame(bar, fg_color="transparent")
        bar_inner.pack(padx=12, pady=10)

        ctk.CTkLabel(bar_inner, text="Имя:").pack(side="left")
        self.name_var = tk.StringVar()
        self.name_entry = ctk.CTkEntry(bar_inner, textvariable=self.name_var,
                                        width=160, corner_radius=8)
        self.name_entry.pack(side="left", padx=(8, 12))
        self.name_entry.bind("<Return>", lambda e: self._add_person())

        ctk.CTkButton(bar_inner, text="Добавить", width=100,
                       corner_radius=8, command=self._add_person
                       ).pack(side="left", padx=3)
        ctk.CTkButton(bar_inner, text="Переименовать", width=130,
                       corner_radius=8, command=self._rename_person
                       ).pack(side="left", padx=3)
        ctk.CTkButton(bar_inner, text="Удалить участника", width=150,
                       corner_radius=8, fg_color="#E06C75",
                       hover_color="#C85A63",
                       command=self._del_person).pack(side="left", padx=3)

        sep = ctk.CTkFrame(bar_inner, width=2, height=28,
                           fg_color="#555555")
        sep.pack(side="left", padx=12)

        ctk.CTkButton(bar_inner, text="Убрать из команды", width=150,
                       corner_radius=8, command=self._remove_from_team
                       ).pack(side="left", padx=3)
        ctk.CTkButton(bar_inner, text="Очистить все команды", width=180,
                       corner_radius=8, fg_color="#E06C75",
                       hover_color="#C85A63",
                       command=self._clear_teams).pack(side="left", padx=3)

    def _style_treeview(self):
        """Стилизация ttk.Treeview под тёмную тему CTk."""
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Dark.Treeview",
                        background="#2B2D3A",
                        foreground="#CDD6F4",
                        fieldbackground="#2B2D3A",
                        borderwidth=0,
                        rowheight=28,
                        font=("Segoe UI", 12))
        style.configure("Dark.Treeview.Heading",
                        background="#363848",
                        foreground="#CDD6F4",
                        font=("Segoe UI", 12, "bold"),
                        borderwidth=0,
                        relief="flat")
        style.map("Dark.Treeview",
                  background=[("selected", "#61AFEF")],
                  foreground=[("selected", "#000000")])
        style.map("Dark.Treeview.Heading",
                  background=[("active", "#444660")])

    def _rebuild_cfg_row(self):
        for w in self.cfg_host.winfo_children():
            w.destroy()
        for i, tv in enumerate(self.team_vars):
            f = ctk.CTkFrame(self.cfg_host, fg_color="transparent")
            f.pack(side="left", padx=(0, 20))
            ctk.CTkLabel(f, text=f"Команда {i+1}:").pack(side="left")
            ctk.CTkEntry(f, textvariable=tv["name"], width=130,
                         corner_radius=8).pack(side="left", padx=6)
            clr_btn = ctk.CTkButton(
                f, text="", width=32, height=32, corner_radius=8,
                fg_color=tv["color"].get(), hover_color=tv["color"].get(),
                command=lambda idx=i: self._pick_color(idx))
            clr_btn.pack(side="left")
            tv["_btn"] = clr_btn

    def _pick_color(self, idx):
        c = colorchooser.askcolor(self.team_vars[idx]["color"].get(),
                                  title=f"Цвет команды {idx+1}")
        if c and c[1]:
            self.team_vars[idx]["color"].set(c[1])
            self.team_vars[idx]["_btn"].configure(
                fg_color=c[1], hover_color=c[1])
            self._vars_to_cfg()

    def _on_count_change(self):
        try:
            cnt = int(self.team_count_var.get())
        except ValueError:
            return
        cnt = max(2, min(5, cnt))
        old = len(self.team_vars)
        if cnt == old:
            return
        if cnt > old:
            for i in range(old, cnt):
                tv = {"name": tk.StringVar(value=f"Команда {i+1}"),
                      "color": tk.StringVar(value=PALETTE[i % len(PALETTE)])}
                self.team_vars.append(tv)
        else:
            self.team_vars = self.team_vars[:cnt]
        for ti in list(self.team_members.keys()):
            if ti >= cnt:
                del self.team_members[ti]
        self._vars_to_cfg()
        self._rebuild_cfg_row()
        self._rebuild_team_lists()

    # ─── Участники: CRUD ─────────────────────────────────────────────────

    def _refresh_persons(self):
        self.persons = {int(r["id"]): r["name"]
                        for r in self.db.all_persons()}
        self._rebuild_free_tree()

    def _rebuild_free_tree(self):
        self.free_tree.delete(*self.free_tree.get_children())
        assigned = set()
        for pids in self.team_members.values():
            assigned.update(pids)
        for pid, name in self.persons.items():
            if pid not in assigned:
                self.free_tree.insert("", "end", iid=str(pid),
                                      values=(pid, name))

    def _add_person(self):
        name = self.name_var.get().strip()
        if not name:
            return
        self.db.add_person(name)
        self.name_var.set("")
        self._refresh_persons()

    def _rename_person(self):
        sel = self.free_tree.selection()
        if not sel:
            for ti, tree in getattr(self, 'team_trees', {}).items():
                s = tree.selection()
                if s:
                    sel = s
                    break
        if not sel:
            return
        pid = int(sel[0])
        new_name = self.name_var.get().strip()
        if not new_name:
            messagebox.showwarning("Ошибка", "Введите новое имя.")
            return
        self.db.rename_person(pid, new_name)
        self.name_var.set("")
        self._refresh_persons()
        self._rebuild_team_lists()

    def _del_person(self):
        sel = self.free_tree.selection()
        if not sel:
            return
        if not messagebox.askyesno("Удаление",
                                   f"Удалить {len(sel)} участник(ов)?"):
            return
        for iid in sel:
            pid = int(iid)
            self.db.remove_person(pid)
            for ti in self.team_members:
                if pid in self.team_members[ti]:
                    self.team_members[ti].remove(pid)
        self._refresh_persons()
        self._rebuild_team_lists()

    # ─── Составы команд ──────────────────────────────────────────────────

    def _rebuild_team_lists(self):
        for w in self.teams_host.winfo_children():
            w.destroy()
        self.team_trees = {}
        cnt = len(self.team_vars)
        for i in range(cnt):
            self.teams_host.columnconfigure(i, weight=1)

        for ti in range(cnt):
            if ti not in self.team_members:
                self.team_members[ti] = []
            col = ctk.CTkFrame(self.teams_host, corner_radius=8,
                               fg_color="#2B2D3A")
            col.grid(row=0, column=ti, sticky="nsew", padx=3, pady=3)

            # Заголовок
            ctk.CTkLabel(col, text=self.team_name(ti),
                         font=("Segoe UI", 14, "bold"),
                         text_color=self.team_color(ti)).pack(
                anchor="w", padx=10, pady=(8, 4))

            # Таблица
            tf = ctk.CTkFrame(col, fg_color="transparent")
            tf.pack(fill="both", expand=True, padx=6, pady=(0, 4))

            tree = ttk.Treeview(tf, columns=("name",), show="headings",
                                selectmode="extended", height=8,
                                style="Dark.Treeview")
            tree.heading("name", text="Игрок")
            tree.column("name", width=120, anchor="w")
            tree.pack(fill="both", expand=True)
            self.team_trees[ti] = tree

            for pid in self.team_members[ti]:
                name = self.persons.get(pid, str(pid))
                tree.insert("", "end", iid=str(pid), values=(name,))

            # Кнопки ПО ЦЕНТРУ
            btn_row = ctk.CTkFrame(col, fg_color="transparent")
            btn_row.pack(pady=(4, 8))

            ctk.CTkButton(btn_row, text="Добавить в команду",
                          width=150, height=30, corner_radius=8,
                          font=("Segoe UI", 12),
                          command=lambda t=ti: self._add_to_team(t)
                          ).pack(side="left", padx=3)
            ctk.CTkButton(btn_row, text="Удалить из команды",
                          width=160, height=30, corner_radius=8,
                          font=("Segoe UI", 12),
                          fg_color="#E06C75", hover_color="#C85A63",
                          command=lambda t=ti: self._remove_from_team_specific(t)
                          ).pack(side="left", padx=3)

        self._rebuild_free_tree()
        self._save_members()

    def _add_to_team(self, ti):
        sel = self.free_tree.selection()
        if not sel:
            return
        for iid in sel:
            pid = int(iid)
            if pid not in self.team_members.get(ti, []):
                self.team_members.setdefault(ti, []).append(pid)
        self._rebuild_team_lists()

    def _remove_from_team_specific(self, ti):
        tree = self.team_trees.get(ti)
        if not tree:
            return
        sel = tree.selection()
        for iid in sel:
            pid = int(iid)
            if pid in self.team_members.get(ti, []):
                self.team_members[ti].remove(pid)
        self._rebuild_team_lists()

    def _remove_from_team(self):
        for ti, tree in self.team_trees.items():
            sel = tree.selection()
            for iid in sel:
                pid = int(iid)
                if pid in self.team_members.get(ti, []):
                    self.team_members[ti].remove(pid)
        self._rebuild_team_lists()

    def _clear_teams(self):
        if not messagebox.askyesno("Очистка", "Очистить все команды?"):
            return
        for ti in self.team_members:
            self.team_members[ti] = []
        self._rebuild_team_lists()


    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Матч»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_match_tab(self):
        root = self.tab_match

        # Верхняя панель
        top = ctk.CTkFrame(root, corner_radius=10)
        top.pack(fill="x", padx=8, pady=(8, 4))

        top_inner = ctk.CTkFrame(top, fg_color="transparent")
        top_inner.pack(fill="x", padx=12, pady=8)

        ctk.CTkButton(top_inner, text="Новый матч", width=130,
                       corner_radius=8, font=("Segoe UI", 14, "bold"),
                       command=self._new_match).pack(side="left")

        self.save_btn = ctk.CTkButton(
            top_inner, text="💾 Сохранить матч", width=180,
            corner_radius=8, font=("Segoe UI", 14, "bold"),
            fg_color="#98C379", hover_color="#7BA862",
            text_color="#000000", state="disabled",
            command=self._manual_save)
        self.save_btn.pack(side="left", padx=(12, 0))

        self.match_info_lbl = ctk.CTkLabel(
            top_inner, text="", font=("Segoe UI", 14, "bold"),
            text_color="#61AFEF")
        self.match_info_lbl.pack(side="right")

        # Табло (компактное)
        self.scoreboard = ctk.CTkFrame(root, corner_radius=10,
                                        fg_color="#1E2030")
        self.scoreboard.pack(fill="x", padx=8, pady=2)

        # Скроллируемая область
        self.m_scroll = ctk.CTkScrollableFrame(
            root, corner_radius=0, fg_color="transparent")
        self.m_scroll.pack(fill="both", expand=True, padx=8, pady=2)

        # Нижняя панель
        bot = ctk.CTkFrame(root, corner_radius=10)
        bot.pack(fill="x", padx=8, pady=(2, 8))

        bot_inner = ctk.CTkFrame(bot, fg_color="transparent")
        bot_inner.pack(fill="x", padx=12, pady=8)

        self.score_lbl = ctk.CTkLabel(bot_inner, text="",
                                       font=("Segoe UI", 12),
                                       text_color="#888888")
        self.score_lbl.pack(anchor="w")

        self.winner_lbl = ctk.CTkLabel(bot_inner, text="Победитель матча:",
                                        font=("Segoe UI", 16, "bold"),
                                        text_color="#888888")
        self.winner_lbl.pack(anchor="w")

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
        for w in self.m_scroll.winfo_children():
            w.destroy()
        self.round_widgets = []
        self.payload = None
        self.saved_sig = None
        self.sectors_var = None
        self.sectors_ui = None

        teams = self._teams_snap()
        if len(teams) < 2:
            self.match_info_lbl.configure(
                text="Нужно минимум 2 команды с участниками")
            self.score_lbl.configure(text="")
            self.winner_lbl.configure(text="", text_color="#888888")
            for w in self.scoreboard.winfo_children():
                w.destroy()
            return

        self.match_info_lbl.configure(text=f"Команд: {len(teams)}")

        # Сетка 3 строки x 3 колонки
        # Строка 0: 3 игры на больше
        # Строка 1: 3 игры на меньше
        # Строка 2: 3 игры на бул
        for row_idx in range(3):
            # Заголовок группы
            ctk.CTkLabel(self.m_scroll, text=ROW_TITLES[row_idx],
                         font=("Segoe UI", 14, "bold"),
                         text_color="#61AFEF").pack(
                anchor="w", padx=6, pady=(8, 2))

            row_f = ctk.CTkFrame(self.m_scroll, fg_color="transparent")
            row_f.pack(fill="x", padx=3)
            for c in range(3):
                row_f.columnconfigure(c, weight=1)

            for col_idx in range(3):
                ri = row_idx * 3 + col_idx
                label, code = ROUND_DEFS[ri]
                cell = ctk.CTkFrame(row_f, fg_color="transparent")
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=2)
                self._make_round(cell, ri + 1, label, code, teams)

        # Сектора
        self._make_sectors_area(teams)
        self._recalc()

    # ─── Создание одного раунда ───────────────────────────────────────────

    def _make_round(self, parent, rno, label, code, teams):
        # Внешний контейнер: grid с 2 колонками (основная игра | доп. игра)
        outer = ctk.CTkFrame(parent, corner_radius=8, fg_color="transparent")
        outer.pack(fill="x", pady=0)
        outer.columnconfigure(0, weight=1)
        outer.columnconfigure(1, weight=0)

        # Левая колонка — основная игра
        box = ctk.CTkFrame(outer, corner_radius=0, fg_color="transparent")
        box.grid(row=0, column=0, sticky="nsew")

        # Заголовок раунда
        ctk.CTkLabel(box, text=f"Игра {rno}: {label}",
                     font=("Segoe UI", 13, "bold"),
                     text_color="#E06C75").pack(
            anchor="w", padx=10, pady=(6, 2))

        body = ctk.CTkFrame(box, fg_color="transparent")
        body.pack(fill="x", padx=10, pady=2)

        entries = {}
        total_vars = {}
        manual_ws = {}

        for team in teams:
            ti = team["idx"]

            # Заголовок команды + сумма
            team_hdr = ctk.CTkFrame(body, fg_color="transparent")
            team_hdr.pack(fill="x", pady=(2, 0))

            ctk.CTkLabel(team_hdr, text=team["name"],
                         font=("Segoe UI", 12, "bold"),
                         text_color=team["color"]).pack(side="left")
            ctk.CTkLabel(team_hdr, text="Σ",
                         font=("Segoe UI", 11),
                         text_color="#888888").pack(side="left", padx=(8, 2))
            tv = tk.StringVar(value="0")
            total_lbl = ctk.CTkLabel(team_hdr, textvariable=tv,
                                     font=("Segoe UI", 13, "bold"),
                                     text_color=team["color"])
            total_lbl.pack(side="left")
            total_vars[ti] = tv

            entries[ti] = {}
            manual_ws[ti] = {}

            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = ctk.CTkFrame(body, fg_color="transparent")
                pf.pack(fill="x", padx=(12, 0), pady=1)

                ctk.CTkLabel(pf, text=pname,
                             font=("Segoe UI", 12)).pack(
                    side="left", padx=(0, 4))

                # Основное поле результата
                rv = tk.StringVar(value="")
                rv.trace_add("write", lambda *_: self._schedule_recalc())
                re = ctk.CTkEntry(pf, textvariable=rv, width=60,
                                  height=28, corner_radius=6,
                                  font=("Segoe UI", 12))
                re.pack(side="left", padx=(0, 4))
                entries[ti][pid] = rv

                # Галочка «вручную»
                mv = tk.BooleanVar(value=False)
                cb = ctk.CTkCheckBox(pf, text="вручную", variable=mv,
                                     width=20, height=20, corner_radius=4,
                                     font=("Segoe UI", 11),
                                     checkbox_width=18, checkbox_height=18)
                cb.pack(side="left", padx=(0, 4))

                # Фрейм для 3 дротиков (скрыт)
                df = ctk.CTkFrame(pf, fg_color="transparent")
                dvars = []
                for d in range(3):
                    dv = tk.StringVar(value="")
                    ctk.CTkEntry(df, textvariable=dv, width=40,
                                 height=26, corner_radius=6,
                                 font=("Segoe UI", 11),
                                 placeholder_text=f"Д{d+1}"
                                 ).pack(side="left", padx=1)
                    dvars.append(dv)
                ok_btn = ctk.CTkButton(
                    df, text="OK", width=36, height=26,
                    corner_radius=6, font=("Segoe UI", 11),
                    command=lambda dvs=dvars, r=rv, dfr=df, m=mv,
                                   re2=re, mc=code:
                        self._apply_darts(dvs, r, dfr, m, re2, mc))
                ok_btn.pack(side="left", padx=2)

                mw_data = {"frame": df, "dvars": dvars, "mvar": mv,
                           "entry": re}
                manual_ws[ti][pid] = mw_data

                mv.trace_add("write", lambda *_, mw=mw_data, r=rv:
                    self._toggle_manual(mw, r))

        # Разница + победитель
        info_f = ctk.CTkFrame(box, fg_color="transparent")
        info_f.pack(fill="x", padx=10, pady=(0, 2))

        diff_var = tk.StringVar(value="Разница: —")
        ctk.CTkLabel(info_f, textvariable=diff_var,
                     font=("Segoe UI", 11),
                     text_color="#888888").pack(anchor="w")

        win_var = tk.StringVar(value="Победитель: —")
        win_lbl = ctk.CTkLabel(info_f, textvariable=win_var,
                               font=("Segoe UI", 12, "bold"),
                               text_color="#888888")
        win_lbl.pack(anchor="w")

        # Правая колонка — хост для доп. игры (рядом с основной)
        tb_host = ctk.CTkFrame(outer, fg_color="transparent")
        tb_host.grid(row=0, column=1, sticky="nsew", padx=(4, 0))
        tb_res = tk.StringVar(value="")
        tb_win = tk.StringVar(value="")

        rw = {
            "rno": rno, "label": label, "code": code, "teams": teams,
            "entries": entries, "totals": total_vars, "manual": manual_ws,
            "diff": diff_var, "win_var": win_var, "win_lbl": win_lbl,
            "tb_host": tb_host, "tb_res": tb_res, "tb_win": tb_win,
            "tb_entries": None, "tb_indices": None, "tb_win_lbl": None,
        }
        self.round_widgets.append(rw)

    # ─── Ручной ввод 3 дротиков ──────────────────────────────────────────

    def _toggle_manual(self, mw, result_var):
        if mw["mvar"].get():
            mw["entry"].configure(state="disabled")
            mw["frame"].pack(side="left", padx=(4, 0))
            for dv in mw["dvars"]:
                dv.set("")
        else:
            mw["frame"].pack_forget()
            mw["entry"].configure(state="normal")

    def _apply_darts(self, dvars, result_var, darts_frame, manual_var,
                     result_entry, mode):
        total = 0
        neg_ok = (mode == "bull")
        for dv in dvars:
            t = dv.get().strip()
            if not t:
                continue
            try:
                v = int(t)
                if not neg_ok and v < 0:
                    v = 0
                total += v
            except ValueError:
                pass
        result_var.set(str(total))
        darts_frame.pack_forget()
        manual_var.set(False)
        result_entry.configure(state="normal")


    # ═══════════════════════════════════════════════════════════════════════
    #  Сектора
    # ═══════════════════════════════════════════════════════════════════════

    def _make_sectors_area(self, teams):
        wrap = ctk.CTkFrame(self.m_scroll, fg_color="transparent")
        wrap.pack(fill="x", pady=(10, 0))

        # Галочка по центру
        self.sectors_var = tk.BooleanVar(value=False)
        cb_f = ctk.CTkFrame(wrap, fg_color="transparent")
        cb_f.pack(anchor="center")
        ctk.CTkCheckBox(cb_f, text="  Сектора (2 дополнительные игры)  ",
                        variable=self.sectors_var,
                        corner_radius=6, font=("Segoe UI", 14, "bold"),
                        command=lambda: self._toggle_sectors(teams)).pack()

        # Хост для блока секторов (скрыт)
        self.sectors_host = ctk.CTkFrame(wrap, fg_color="transparent")
        self.sectors_ui = None  # будет список из 2 элементов

    def _toggle_sectors(self, teams):
        if self.sectors_var.get():
            self._build_sectors(teams)
        else:
            for w in self.sectors_host.winfo_children():
                w.destroy()
            self.sectors_host.pack_forget()
            self.sectors_ui = None
            self._schedule_recalc()

    def _build_one_sector_box(self, parent, game_no, teams):
        """Build one sector game box, return its UI dict."""
        box = ctk.CTkFrame(parent, corner_radius=10)
        box.pack(padx=10, pady=6, side="left", anchor="n")

        ctk.CTkLabel(box, text=f"Сектора {game_no}",
                     font=("Segoe UI", 15, "bold"),
                     text_color="#61AFEF").pack(
            anchor="center", padx=16, pady=(10, 4))

        # Поля секторов
        sf = ctk.CTkFrame(box, fg_color="transparent")
        sf.pack(pady=(0, 6), anchor="center")
        ctk.CTkLabel(sf, text="Секторы:").pack(side="left")
        sec_vars = []
        for i in range(3):
            sv = tk.StringVar(value="")
            ctk.CTkEntry(sf, textvariable=sv, width=60, height=28,
                         corner_radius=6, font=("Segoe UI", 12),
                         placeholder_text=f"С{i+1}").pack(
                side="left", padx=4)
            sec_vars.append(sv)

        # Команды
        teams_f = ctk.CTkFrame(box, fg_color="transparent")
        teams_f.pack(pady=(0, 6), anchor="center")

        entries = {}
        total_vars = {}

        for team in teams:
            ti = team["idx"]
            tf = ctk.CTkFrame(teams_f, corner_radius=8, fg_color="#2B2D3A")
            tf.pack(side="left", padx=10, anchor="n", pady=4)

            ctk.CTkLabel(tf, text=team["name"],
                         font=("Segoe UI", 13, "bold"),
                         text_color=team["color"]).pack(
                anchor="w", padx=10, pady=(8, 2))

            tv = tk.StringVar(value="0")
            total_vars[ti] = tv

            entries[ti] = {}
            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = ctk.CTkFrame(tf, fg_color="transparent")
                pf.pack(fill="x", padx=10, pady=1)
                ctk.CTkLabel(pf, text=pname,
                             font=("Segoe UI", 12)).pack(
                    side="left", padx=(0, 4))
                ev = tk.StringVar(value="")
                ev.trace_add("write", lambda *_: self._schedule_recalc())
                ctk.CTkEntry(pf, textvariable=ev, width=50, height=26,
                             corner_radius=6,
                             font=("Segoe UI", 12)).pack(side="left")
                entries[ti][pid] = ev

            # Итого
            tot_f = ctk.CTkFrame(tf, fg_color="transparent")
            tot_f.pack(fill="x", padx=10, pady=(4, 8))
            ctk.CTkLabel(tot_f, text="Итого:").pack(side="left", padx=(0, 4))
            ctk.CTkLabel(tot_f, textvariable=tv,
                         font=("Segoe UI", 13, "bold"),
                         text_color=team["color"]).pack(side="left")

        # Победитель секторов
        sw_var = tk.StringVar(value="Победитель: —")
        sw_lbl = ctk.CTkLabel(box, textvariable=sw_var,
                              font=("Segoe UI", 14, "bold"),
                              text_color="#888888")
        sw_lbl.pack(pady=(4, 10), anchor="center")

        return {
            "teams": teams,
            "sec_vars": sec_vars,
            "entries": entries,
            "total_vars": total_vars,
            "win_var": sw_var,
            "win_lbl": sw_lbl,
        }

    def _build_sectors(self, teams):
        for w in self.sectors_host.winfo_children():
            w.destroy()
        self.sectors_host.pack(fill="x", pady=(8, 0))

        # Центрирующий контейнер для 2 табличек рядом
        center = ctk.CTkFrame(self.sectors_host, fg_color="transparent")
        center.pack(anchor="center")

        self.sectors_ui = []
        for g in range(2):
            ui = self._build_one_sector_box(center, g + 1, teams)
            self.sectors_ui.append(ui)

        self._schedule_recalc()

    # ═══════════════════════════════════════════════════════════════════════
    #  Пересчёт матча
    # ═══════════════════════════════════════════════════════════════════════

    def _schedule_recalc(self):
        if self._recalc_id is not None:
            self.after_cancel(self._recalc_id)
        self._recalc_id = self.after(80, self._recalc)

    def _recalc(self):
        self._recalc_id = None
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
                n_active = 0

                for pid in team["members"]:
                    var = rw["entries"].get(ti, {}).get(pid)
                    if var is None:
                        has_empty = True
                        scores[ti][pid] = 0
                        continue
                    txt = var.get().strip()
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
                        n_active += 1
                    except ValueError:
                        has_empty = True
                        scores[ti][pid] = 0

                totals[ti] = team_sum
                raw_totals[ti] = raw_totals.get(ti, 0) + team_sum

            if has_empty:
                all_filled = False

            # Обновляем суммы
            for ti, tv in rw["totals"].items():
                tv.set(str(totals.get(ti, 0)))

            # Определяем победителя раунда
            winner = self._round_winner(totals, code)
            diff = self._round_diff(totals)
            rw["diff"].set(f"Разница: {diff}" if diff is not None
                           else "Разница: —")

            # Доп. игра (тайбрейк)
            tb_scores = {}
            final_winner = winner
            if winner is None and not has_empty and len(totals) >= 2:
                tied = self._tied_teams(totals, code)
                if len(tied) >= 2:
                    tb_scores, tb_winner = self._handle_tiebreak(
                        rw, teams, tied, code)
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
                rw["win_var"].set(f"Победитель: {name}")
                rw["win_lbl"].configure(text_color=color)
            elif has_empty:
                rw["win_var"].set("Победитель: —")
                rw["win_lbl"].configure(text_color="#888888")
                all_decided = False
            else:
                rw["win_var"].set("Победитель: ничья")
                rw["win_lbl"].configure(text_color="#E5C07B")
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

        # --- Сектора (2 игры) ---
        sectors_data = None
        sectors_winners = []
        if self.sectors_var and self.sectors_var.get() and self.sectors_ui:
            sectors_data, sectors_winners = self._calc_sectors()
            for sw in sectors_winners:
                if sw is not None:
                    wins[sw] += 1

        # --- Табло ---
        self._draw_scoreboard(teams, wins)

        # --- Итоги ---
        standings = sorted(wins.items(), key=lambda x: x[1], reverse=True)
        score_parts = [f"{self.team_name(ti)} {w}" for ti, w in standings]
        raw_parts = [f"{self.team_name(ti)} {raw_totals.get(ti, 0)}"
                     for ti, _ in standings]
        self.score_lbl.configure(
            text=f"Победы: {' | '.join(score_parts)}   "
                 f"Сумма: {' | '.join(raw_parts)}")

        # --- Победитель ---
        leaders = ([ti for ti, w in standings if w == standings[0][1]]
                   if standings else [])

        undecided = sum(
            1 for rw in self.round_widgets
            if rw["win_var"].get().endswith("ничья")
            or rw["win_var"].get().endswith("—"))

        # Считаем каждую секторную игру как нерешённый раунд если включены но нет победителя
        if self.sectors_var and self.sectors_var.get():
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

        # Если сектора включены, но не все заполнены — не complete
        if (self.sectors_var and self.sectors_var.get()
                and any(sw is None for sw in sectors_winners)
                and not insurmountable):
            complete = False

        if complete:
            w_idx = leaders[0]
            w_name = self.team_name(w_idx)
            w_color = self.team_color(w_idx)
            parts = []
            parts.append("9 игр")
            n_sector_wins = sum(1 for sw in sectors_winners if sw is not None)
            if n_sector_wins > 0:
                parts.append(f"сектора ({n_sector_wins})")
            reason = " + ".join(parts)
        else:
            w_name = ""
            w_color = "#888888"
            reason = ""

        self.winner_lbl.configure(
            text=f"Победитель матча: {w_name}" if w_name
                 else "Победитель матча:",
            text_color=w_color)

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

        # Кнопка сохранения
        if complete:
            self.save_btn.configure(state="normal")
        else:
            self.save_btn.configure(state="disabled")

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

    # ─── Доп. игра (по 1 дротику) ────────────────────────────────────────

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
            for pid_str, var in pmap.items():
                pid = int(pid_str)
                txt = var.get().strip()
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
            rw["tb_win"].set("Доп: —")
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].configure(text_color="#888888")
            return tb_scores, None

        sums = {ti: tb_scores[ti].pop("_sum", 0) for ti in tb_scores}
        winner = self._round_winner(sums, code)
        if winner is not None:
            name = self.team_name(winner)
            rw["tb_win"].set(f"Доп: {name}")
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].configure(
                    text_color=self.team_color(winner))
        else:
            rw["tb_win"].set("Доп: ничья")
            if rw["tb_win_lbl"]:
                rw["tb_win_lbl"].configure(text_color="#E5C07B")
        return tb_scores, winner

    def _build_tiebreak(self, rw, teams, tied_indices, code):
        host = rw["tb_host"]
        for w in host.winfo_children():
            w.destroy()

        ctk.CTkLabel(host, text="Доп. игра",
                     font=("Segoe UI", 11, "bold"),
                     text_color="#E5C07B").pack(anchor="w", pady=(4, 0))
        ctk.CTkLabel(host, text="(по 1 дротику)",
                     font=("Segoe UI", 10),
                     text_color="#888888").pack(anchor="w", pady=(0, 2))

        tb_entries = {}
        for team in teams:
            ti = team["idx"]
            if ti not in tied_indices:
                continue

            ctk.CTkLabel(host, text=team["name"],
                         font=("Segoe UI", 11, "bold"),
                         text_color=team["color"]).pack(anchor="w")

            tb_entries[str(ti)] = {}
            for pid in team["members"]:
                pname = self.persons.get(pid, str(pid))
                pf = ctk.CTkFrame(host, fg_color="transparent")
                pf.pack(fill="x", pady=1)
                ctk.CTkLabel(pf, text=pname,
                             font=("Segoe UI", 10)).pack(
                    side="left", padx=(0, 2))
                ev = tk.StringVar(value="")
                ev.trace_add("write", lambda *_: self._schedule_recalc())
                ctk.CTkEntry(pf, textvariable=ev, width=45, height=24,
                             corner_radius=6,
                             font=("Segoe UI", 10)).pack(side="left")
                tb_entries[str(ti)][str(pid)] = ev

        rw["tb_entries"] = tb_entries

        rw["tb_win_lbl"] = ctk.CTkLabel(host, textvariable=rw["tb_win"],
                                         font=("Segoe UI", 11),
                                         text_color="#888888")
        rw["tb_win_lbl"].pack(anchor="w")

    def _clear_tiebreak(self, rw):
        for w in rw["tb_host"].winfo_children():
            w.destroy()
        rw["tb_entries"] = None
        rw["tb_win_lbl"] = None
        rw["tb_win"].set("")

    # ─── Расчёт секторов ─────────────────────────────────────────────────

    def _calc_one_sector(self, ui):
        """Calculate one sector game, return (data, winner_idx)."""
        if not ui:
            return None, None

        totals = {}
        any_filled = False

        for team in ui["teams"]:
            ti = team["idx"]
            team_total = 0
            for pid, var in ui["entries"][ti].items():
                txt = var.get().strip()
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
            ui["total_vars"][ti].set(str(team_total))

        if not any_filled:
            ui["win_var"].set("Победитель: —")
            ui["win_lbl"].configure(text_color="#888888")
            return None, None

        max_val = max(totals.values())
        leaders = [ti for ti, v in totals.items() if v == max_val]

        scores_data = {}
        for team in ui["teams"]:
            ti = team["idx"]
            scores_data[str(ti)] = {}
            for pid, var in ui["entries"][ti].items():
                txt = var.get().strip()
                if is_skip(txt):
                    scores_data[str(ti)][str(pid)] = SKIP
                elif txt:
                    try:
                        scores_data[str(ti)][str(pid)] = max(0, int(txt))
                    except ValueError:
                        scores_data[str(ti)][str(pid)] = 0
                else:
                    scores_data[str(ti)][str(pid)] = 0

        sec_names = [sv.get().strip() for sv in ui["sec_vars"]]

        if len(leaders) == 1:
            w_idx = leaders[0]
            name = self.team_name(w_idx)
            color = self.team_color(w_idx)
            ui["win_var"].set(f"Победитель: {name}")
            ui["win_lbl"].configure(text_color=color)
            data = {"totals": totals, "scores": scores_data,
                    "sectors": sec_names, "winner": w_idx}
            return data, w_idx
        else:
            ui["win_var"].set("Победитель: ничья")
            ui["win_lbl"].configure(text_color="#E5C07B")
            data = {"totals": totals, "scores": scores_data,
                    "sectors": sec_names, "winner": None}
            return data, None

    def _calc_sectors(self):
        """Calculate both sector games, return (list_of_data, list_of_winners)."""
        if not self.sectors_ui:
            return None, []
        all_data = []
        winners = []
        for ui in self.sectors_ui:
            data, w = self._calc_one_sector(ui)
            all_data.append(data)
            winners.append(w)
        return all_data, winners

    # ─── Табло ────────────────────────────────────────────────────────────

    def _draw_scoreboard(self, teams, wins):
        for w in self.scoreboard.winfo_children():
            w.destroy()
        if not teams:
            return

        row = ctk.CTkFrame(self.scoreboard, fg_color="transparent")
        row.pack(pady=6)

        for i, team in enumerate(teams):
            ti = team["idx"]
            w = wins.get(ti, 0)
            if i > 0:
                ctk.CTkLabel(row, text="—",
                             font=("Segoe UI", 24, "bold"),
                             text_color="#555555").pack(
                    side="left", padx=12)

            tf = ctk.CTkFrame(row, fg_color="transparent")
            tf.pack(side="left", padx=12)
            ctk.CTkLabel(tf, text=team["name"],
                         font=("Segoe UI", 12),
                         text_color=team["color"]).pack()
            ctk.CTkLabel(tf, text=str(w),
                         font=("Segoe UI", 32, "bold"),
                         text_color=team["color"]).pack()


    # ═══════════════════════════════════════════════════════════════════════
    #  Сохранение
    # ═══════════════════════════════════════════════════════════════════════

    def _manual_save(self):
        if not self.payload or not self.payload.get("winner"):
            messagebox.showwarning("Сохранение", "Матч ещё не завершён.")
            return
        sig = json.dumps(self.payload, sort_keys=True, ensure_ascii=False)
        if sig == self.saved_sig:
            messagebox.showinfo("Сохранение", "Этот результат уже сохранён.")
            return
        w = self.payload["winner"]
        if messagebox.askyesno("Сохранение",
                               f"Сохранить матч?\nПобедитель: {w}"):
            self.db.save_match(self.payload)
            self.saved_sig = sig
            self._refresh_history()
            messagebox.showinfo("Готово", "Матч сохранён!")

    # ═══════════════════════════════════════════════════════════════════════
    #  Вкладка «Статистика»
    # ═══════════════════════════════════════════════════════════════════════

    def _build_stats_tab(self):
        root = self.tab_stats

        # Статистика
        st_frame = ctk.CTkFrame(root, corner_radius=10)
        st_frame.pack(fill="both", expand=True, padx=8, pady=(8, 4))

        ctk.CTkLabel(st_frame, text="Статистика игроков",
                     font=("Segoe UI", 14, "bold")).pack(
            anchor="w", padx=12, pady=(10, 4))

        st_tree_frame = ctk.CTkFrame(st_frame, fg_color="transparent")
        st_tree_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("name", "n", "avg_max", "r_max", "avg_min", "r_min",
                "avg_bull", "r_bull", "avg_p", "overall")
        self.st_tree = ttk.Treeview(st_tree_frame, columns=cols,
                                    show="headings", height=10,
                                    style="Dark.Treeview")
        headers = [
            ("name", "Имя", 130), ("n", "Матчей", 60),
            ("avg_max", "Ср. макс", 80), ("r_max", "Ранг", 50),
            ("avg_min", "Ср. мин", 80), ("r_min", "Ранг", 50),
            ("avg_bull", "Ср. бул", 80), ("r_bull", "Ранг", 50),
            ("avg_p", "Ср. место", 80), ("overall", "Общий", 60),
        ]
        for cid, text, w in headers:
            self.st_tree.heading(cid, text=text)
            self.st_tree.column(
                cid, width=w,
                anchor="center" if cid != "name" else "w")

        st_sb = ctk.CTkScrollbar(st_tree_frame,
                                  command=self.st_tree.yview)
        self.st_tree.configure(yscrollcommand=st_sb.set)
        self.st_tree.pack(side="left", fill="both", expand=True)
        st_sb.pack(side="right", fill="y")

        # История
        hi_frame = ctk.CTkFrame(root, corner_radius=10)
        hi_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        hi_top = ctk.CTkFrame(hi_frame, fg_color="transparent")
        hi_top.pack(fill="x", padx=12, pady=(10, 4))

        ctk.CTkLabel(hi_top, text="История матчей",
                     font=("Segoe UI", 14, "bold")).pack(side="left")

        btn_row = ctk.CTkFrame(hi_top, fg_color="transparent")
        btn_row.pack(side="right")

        ctk.CTkButton(btn_row, text="Показать", width=100,
                       corner_radius=8, command=self._show_match
                       ).pack(side="left", padx=3)
        ctk.CTkButton(btn_row, text="Удалить выбранные", width=160,
                       corner_radius=8, fg_color="#E06C75",
                       hover_color="#C85A63",
                       command=self._delete_matches
                       ).pack(side="left", padx=3)
        ctk.CTkButton(btn_row, text="Обновить", width=100,
                       corner_radius=8, command=self._refresh_history
                       ).pack(side="left", padx=3)

        hi_tree_frame = ctk.CTkFrame(hi_frame, fg_color="transparent")
        hi_tree_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.hi_tree = ttk.Treeview(
            hi_tree_frame,
            columns=("date", "summary", "winner", "reason"),
            show="headings", selectmode="extended", height=8,
            style="Dark.Treeview")
        for cid, text, w in (
            ("date", "Дата", 150), ("summary", "Результат", 500),
            ("winner", "Победитель", 150), ("reason", "Определение", 200),
        ):
            self.hi_tree.heading(cid, text=text)
            self.hi_tree.column(cid, width=w, anchor="w")

        hi_sb = ctk.CTkScrollbar(hi_tree_frame,
                                  command=self.hi_tree.yview)
        self.hi_tree.configure(yscrollcommand=hi_sb.set)
        self.hi_tree.pack(side="left", fill="both", expand=True)
        hi_sb.pack(side="right", fill="y")

        self.hi_tree.bind("<Double-1>", lambda e: self._show_match())

    # ─── Обновление статистики и истории ──────────────────────────────────

    def _refresh_history(self):
        # Статистика
        self.st_tree.delete(*self.st_tree.get_children())
        for r in self.db.calc_stats():
            self.st_tree.insert("", "end", values=(
                r["name"], r["n"],
                fmt(r["avg_max"]), fmt(r["r_max"]),
                fmt(r["avg_min"]), fmt(r["r_min"]),
                fmt(r["avg_bull"]), fmt(r["r_bull"]),
                fmt(r["avg_p"]), fmt(r["overall"]),
            ))

        # История
        self.hi_tree.delete(*self.hi_tree.get_children())
        for row in self.db.all_matches():
            self.hi_tree.insert("", "end", iid=str(row["id"]), values=(
                row["played_at"] or "",
                row["summary"] or "",
                row["winner"] or "—",
                row["reason"] or "",
            ))

    # ─── Просмотр матча ──────────────────────────────────────────────────

    def _show_match(self):
        sel = self.hi_tree.selection()
        if not sel:
            return
        mid = int(sel[0])
        row = self.db.match_by_id(mid)
        if not row:
            return
        try:
            p = json.loads(row["payload"])
        except Exception:
            messagebox.showerror("Ошибка",
                                 "Не удалось прочитать данные матча.")
            return

        win = ctk.CTkToplevel(self)
        win.title(f"Матч #{mid} — {row['played_at']}")
        win.geometry("800x700")

        # Скроллируемое содержимое
        scroll = ctk.CTkScrollableFrame(win, corner_radius=0)
        scroll.pack(fill="both", expand=True, padx=10, pady=10)

        # Заголовок
        ctk.CTkLabel(scroll, text=f"Матч #{mid}",
                     font=("Segoe UI", 20, "bold"),
                     text_color="#61AFEF").pack(
            anchor="w", padx=12, pady=(12, 2))
        ctk.CTkLabel(scroll, text=f"Дата: {row['played_at']}",
                     font=("Segoe UI", 12),
                     text_color="#888888").pack(anchor="w", padx=12)
        ctk.CTkLabel(scroll, text=f"Победитель: {row['winner'] or '—'}",
                     font=("Segoe UI", 16, "bold"),
                     text_color="#98C379").pack(anchor="w", padx=12)
        ctk.CTkLabel(scroll, text=f"Определение: {row['reason'] or '—'}",
                     font=("Segoe UI", 12),
                     text_color="#888888").pack(
            anchor="w", padx=12, pady=(0, 10))

        # Карта команд
        team_map = {}
        for t in p.get("teams", []):
            team_map[str(t["idx"])] = t

        # Раунды
        for rnd in p.get("rounds", []):
            rf = ctk.CTkFrame(scroll, corner_radius=8)
            rf.pack(fill="x", padx=12, pady=3)

            ctk.CTkLabel(rf, text=f"Игра {rnd['rno']}: {rnd['label']}",
                         font=("Segoe UI", 13, "bold"),
                         text_color="#E06C75").pack(
                anchor="w", padx=10, pady=(6, 2))

            winner = rnd.get("winner")
            for ti_str, smap in rnd.get("scores", {}).items():
                team = team_map.get(ti_str, {})
                total = rnd.get("totals", {}).get(int(ti_str), 0)
                ctk.CTkLabel(
                    rf, text=f"{team.get('name', '?')} — Σ {total}",
                    font=("Segoe UI", 12, "bold"),
                    text_color=team.get("color", "#CDD6F4")).pack(
                    anchor="w", padx=10, pady=(2, 0))
                for pid_s, val in smap.items():
                    pname = self.persons.get(int(pid_s), pid_s)
                    disp = ("x (пропуск)" if val == SKIP or val == "x"
                            else str(val))
                    ctk.CTkLabel(rf, text=f"    {pname}: {disp}",
                                 font=("Segoe UI", 11)).pack(
                        anchor="w", padx=10)

            if winner is not None:
                wt = team_map.get(str(winner), {})
                ctk.CTkLabel(
                    rf, text=f"Победитель: {wt.get('name', '?')}",
                    font=("Segoe UI", 12, "bold"),
                    text_color=wt.get("color", "#CDD6F4")).pack(
                    anchor="w", padx=10, pady=(2, 6))
            else:
                ctk.CTkLabel(rf, text="Победитель: ничья",
                             font=("Segoe UI", 12),
                             text_color="#E5C07B").pack(
                    anchor="w", padx=10, pady=(2, 6))

            # Доп. игра
            tb = rnd.get("tb_scores", {})
            if tb:
                ctk.CTkLabel(rf, text="Доп. игра (по 1 дротику):",
                             font=("Segoe UI", 12, "bold"),
                             text_color="#E5C07B").pack(
                    anchor="w", padx=10)
                for ti_str, smap in tb.items():
                    team = team_map.get(ti_str, {})
                    for pid_s, val in smap.items():
                        pname = self.persons.get(int(pid_s), pid_s)
                        disp = ("x (пропуск)" if val == SKIP or val == "x"
                                else str(val))
                        ctk.CTkLabel(
                            rf,
                            text=f"    {team.get('name', '?')} · {pname}: {disp}",
                            font=("Segoe UI", 11)).pack(
                            anchor="w", padx=14)

        # Сектора (может быть список из 2 или один dict для совместимости)
        sectors_raw = p.get("sectors")
        if sectors_raw:
            # Нормализуем: если dict — оборачиваем в список
            if isinstance(sectors_raw, dict):
                sectors_list = [sectors_raw]
            else:
                sectors_list = sectors_raw

            for g_idx, sectors in enumerate(sectors_list):
                if not sectors:
                    continue
                sf = ctk.CTkFrame(scroll, corner_radius=8)
                sf.pack(fill="x", padx=12, pady=(8, 4))

                ctk.CTkLabel(sf, text=f"Сектора {g_idx + 1}",
                             font=("Segoe UI", 14, "bold"),
                             text_color="#61AFEF").pack(
                    anchor="w", padx=10, pady=(8, 2))

                sec_names = sectors.get("sectors", [])
                if sec_names:
                    ctk.CTkLabel(sf, text=f"Секторы: {', '.join(str(s) for s in sec_names)}",
                                 font=("Segoe UI", 12)).pack(
                        anchor="w", padx=10, pady=(0, 2))

                for ti_str, smap in sectors.get("scores", {}).items():
                    team = team_map.get(ti_str, {})
                    total = sectors.get("totals", {}).get(int(ti_str), 0)
                    ctk.CTkLabel(
                        sf, text=f"{team.get('name', '?')} — Σ {total}",
                        font=("Segoe UI", 12, "bold"),
                        text_color=team.get("color", "#CDD6F4")).pack(
                        anchor="w", padx=10, pady=(2, 0))
                    for pid_s, val in smap.items():
                        pname = self.persons.get(int(pid_s), pid_s)
                        disp = ("x (пропуск)" if val == SKIP or val == "x"
                                else str(val))
                        ctk.CTkLabel(sf, text=f"    {pname}: {disp}",
                                     font=("Segoe UI", 11)).pack(
                            anchor="w", padx=10)

                sw = sectors.get("winner")
                if sw is not None:
                    wt = team_map.get(str(sw), {})
                    ctk.CTkLabel(
                        sf, text=f"Победитель: {wt.get('name', '?')}",
                        font=("Segoe UI", 13, "bold"),
                        text_color=wt.get("color", "#CDD6F4")).pack(
                        anchor="w", padx=10, pady=(4, 8))

        # Итог
        ctk.CTkLabel(scroll, text=p.get("summary", ""),
                     font=("Segoe UI", 12),
                     text_color="#888888").pack(
            anchor="w", padx=12, pady=(10, 12))

    # ─── Удаление матчей ─────────────────────────────────────────────────

    def _delete_matches(self):
        sel = self.hi_tree.selection()
        if not sel:
            messagebox.showinfo(
                "Удаление",
                "Выберите матчи для удаления.\n"
                "Используйте Ctrl+клик для множественного выбора.")
            return
        n = len(sel)
        word = "матч" if n == 1 else f"матчей: {n}"
        if not messagebox.askyesno("Удаление", f"Удалить {word}?"):
            return
        ids = [int(iid) for iid in sel]
        self.db.delete_matches(ids)
        self._refresh_history()

    # ─── Выход ────────────────────────────────────────────────────────────

    def _quit(self):
        self._vars_to_cfg()
        self._save_members()
        self.destroy()


# ═══════════════════════════════════════════════════════════════════════════════
#  Запуск
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app = App()
    app.mainloop()
