"""
Extractor для GitLab API.
Извлекает статистику строк кода (LOC) из GitLab проектов.
"""
from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

import gitlab
from git import Repo


INCLUDE_EXTS: dict[str, str] = {
    ".cs": "C#",
    ".py": "Python",
    ".xaml": "XAML",
    ".yml": "YAML",
    ".yaml": "YAML",
    ".html": "HTML",
    ".htm": "HTML",
    ".ts": "TypeScript",
    ".tsx": "TypeScript",
    ".js": "JavaScript",
    ".jsx": "JavaScript",
    ".css": "CSS",
    ".scss": "SCSS",
    ".sass": "SASS",
    ".sln": "Solution",
}

BRACE_ONLY: set[str] = {"{", "}"}  # одинокие фигурные скобки
MAX_WORKERS: int = 8


def extract_gitlab_lines(
    gitlab_url: str,
    gitlab_token: str,
    output_path: str,
    max_workers: int = MAX_WORKERS,
    **context
) -> str:
    """
    Экспортирует статистику строк кода из GitLab проектов.

    Args:
        gitlab_url: URL GitLab сервера (например, "http://192.168.42.188:13080")
        gitlab_token: Private token для GitLab API
        output_path: Путь для сохранения JSON файла
        max_workers: Количество потоков для параллельной обработки
    """

    def url_with_token(url: str) -> str:
        """Встраивает private_token (PAT) в URL репозитория."""
        proto, rest = (
            ("http://", url.removeprefix("http://"))
            if url.startswith("http://")
            else ("https://", url.removeprefix("https://"))
        )
        if "@" in rest.split("/", 1)[0]:  # убрать git@host
            rest = rest.split("@", 1)[1]
        return f"{proto}oauth2:{gitlab_token}@{rest}"

    def list_branches(pr) -> set[str]:
        """Возвращает master, default и все ветки, содержащие 'dev'."""
        branches: set[str] = {pr.default_branch or "main", "master"}
        for br in pr.branches.list(iterator=True, per_page=100):
            if "dev" in br.name.lower():
                branches.add(br.name)
        return branches

    def branch_loc(repo: Repo, ref: str) -> Dict[str, object] | None:
        """Считает строки кода на указанном ref (ветка/коммит)."""
        try:
            paths = repo.git.ls_tree("-r", "--name-only", ref).splitlines()
        except Exception:
            return None

        lang_loc: dict[str, int] = defaultdict(int)
        for path in paths:
            ext = Path(path).suffix.lower()
            if ext not in INCLUDE_EXTS:
                continue
            try:
                blob = repo.git.show(f"{ref}:{path}")
            except Exception:
                continue

            lang = INCLUDE_EXTS[ext]
            cs_file = ext == ".cs"
            for line in blob.splitlines():
                line = line.strip()
                if not line:
                    continue
                if cs_file and line in BRACE_ONLY:
                    continue
                lang_loc[lang] += 1

        if not lang_loc:
            return None
        return {"loc": sum(lang_loc.values()), "langs": lang_loc}

    def process_project(pr) -> Dict[str, object]:
        """Обрабатывает проект GitLab, возвращая данные для JSON отчёта."""
        with tempfile.TemporaryDirectory() as tmp:
            try:
                repo = Repo.clone_from(
                    url_with_token(pr.http_url_to_repo),
                    tmp,
                    depth=1,
                    no_single_branch=True,  # один набор объектов на все ветки
                    quiet=True,
                )
            except Exception:
                return {
                    "id": pr.id,
                    "name": pr.path_with_namespace,
                    "chosen_branch": None,
                    "loc_by_language": {},
                }

            branch_stats: Dict[str, Dict[str, object]] = {}
            for br in list_branches(pr):
                if br in branch_stats:
                    continue
                stat = branch_loc(repo, br)
                if stat:
                    branch_stats[br] = stat

            if not branch_stats:
                return {
                    "id": pr.id,
                    "name": pr.path_with_namespace,
                    "chosen_branch": None,
                    "loc_by_language": {},
                }

            chosen_branch = max(branch_stats.items(), key=lambda kv: kv[1]["loc"])[0]
            return {
                "id": pr.id,
                "name": pr.path_with_namespace,
                "chosen_branch": chosen_branch,
                "loc_by_language": branch_stats[chosen_branch]["langs"],
            }

    # === Основная логика ===
    print("Начало экспорта статистики строк кода из GitLab проектов")

    gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token, keep_base_url=True)
    gl.auth()

    projects = list(gl.projects.list(iterator=True, per_page=100))
    result: List[Dict[str, object]] = []

    print(f"Обработка {len(projects)} проектов...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_project, pr): pr.id for pr in projects}

        for i, fut in enumerate(as_completed(futures), 1):
            result.append(fut.result())
            if i % 10 == 0:
                print(f"Обработано проектов: {i}/{len(projects)}")

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Экспорт завершен. Сохранено проектов: {len(result)} в {output_path}")
    return output_path
