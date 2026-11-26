"""A real, guided terminal demo for a 1–2 minute screen recording.

Only the clients run concurrently. All results come from the existing engine
and its single-threaded HTTP server. Secrets stay in memory, never on screen.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import http.client
import json
from pathlib import Path
import subprocess
import sys
import threading
import uuid

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from restaurant_db import Database
from restaurant_db.auth import issue_token


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


class Demo:
    def __init__(self, root: Path, automatic: bool):
        self.directory = root / f"{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex}"
        self.directory.mkdir(parents=True, exist_ok=False)
        self.data = self.directory / "data"
        self.auth_file = self.directory / "auth" / "users.json"
        self.automatic = automatic
        self.process = None
        self.port = None
        self.report = {}
        self.tokens = {user: issue_token(self.auth_file, user) for user in ("alice", "bob")}

    def stage(self, title: str) -> None:
        if not self.automatic:
            input("\nEnter — следующий шаг… ")
        print(f"\n=== {title} ===", flush=True)

    def start_server(self) -> None:
        self.process = subprocess.Popen(
            [sys.executable, "-m", "restaurant_db", "serve", "--data", str(self.data),
             "--auth-file", str(self.auth_file), "--port", "0"],
            cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        line = self.process.stdout.readline()
        require(bool(line), "Сервер не запустился; запись демо нужно остановить.")
        ready = json.loads(line)
        self.port = int(ready["listening"].rsplit(":", 1)[1])

    def stop_server(self) -> None:
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.communicate(timeout=10)
            self.process = None

    def post(self, user: str, request_id: str, command: dict):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            body = json.dumps({"request_id": request_id, "command": command}).encode()
            connection.request("POST", "/commands", body, {
                "Content-Type": "application/json", "Authorization": f"Bearer {self.tokens[user]}"})
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def slots(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            connection.request("GET", "/slots")
            response = connection.getresponse()
            require(response.status == 200, "Не удалось получить слоты.")
            return json.loads(response.read())["slots"]
        finally:
            connection.close()

    def run(self) -> None:
        print("RESTAURANT DB — СОБСТВЕННАЯ БАЗА БРОНИРОВАНИЙ")
        print("Один обработчик • журнал на диске • токены пользователей")
        print("Отдельный набор данных; каждый показанный результат проверяется.")
        print("\n=== 1. Подготовленный батч ===")
        with Database(self.data) as db:
            require(db.execute("table", {"op": "add_table", "table_id": "t1", "name": "Window table"})["ok"],
                    "Не удалось создать столик.")
            result = db.execute("batch", {"op": "prepare_batch", "batch_id": "evening", "slots": [
                {"slot_id": "s1", "table_id": "t1", "starts_at": "2030-01-01T18:00:00+02:00",
                 "ends_at": "2030-01-01T20:00:00+02:00"},
                {"slot_id": "s2", "table_id": "t1", "starts_at": "2030-01-01T20:00:00+02:00",
                 "ends_at": "2030-01-01T22:00:00+02:00"}]})
            require(result["ok"] and db.list_slots() == [], "Черновик неожиданно доступен.")
            print("Создан 1 столик и 2 слота. Опубликованных слотов: 0.")

            self.stage("2. Атомарная публикация")
            require(db.execute("publish", {"op": "publish_batch", "batch_id": "evening"})["ok"],
                    "Не удалось опубликовать батч.")
            require(len(db.list_slots()) == 2, "Батч опубликован не полностью.")
        self.start_server()
        require(len(self.slots()) == 2, "HTTP не показывает весь опубликованный батч.")
        print("Один commit → опубликованы сразу 2 слота.")
        print("Запущен настоящий HTTP-сервер с проверкой токенов.")
        self.report["published_slots"] = 2

        self.stage("3. Одновременное бронирование одного слота")
        clients = 20  # A small demonstration, not a capacity benchmark.
        barrier = threading.Barrier(clients)
        command = {"op": "reserve", "slot_id": "s1"}

        def compete(index):
            user = "alice" if index % 2 == 0 else "bob"
            key = f"race-{index}"
            barrier.wait()
            status, result = self.post(user, key, command)
            return user, key, status, result

        with ThreadPoolExecutor(max_workers=clients) as pool:
            results = list(pool.map(compete, range(clients)))
        counts = Counter(result[3]["code"] for result in results)
        require(counts == {"reserved": 1, "slot_taken": clients - 1}, "Наплыв запросов дал неожиданный результат.")
        winner, winning_key, status, booking = next(item for item in results if item[3]["code"] == "reserved")
        require(status == 200 and all(item[2] == 409 for item in results if item[3]["code"] == "slot_taken"),
                "Неверные HTTP-статусы.")
        reservation_id = booking["reservation"]["reservation_id"]
        print("20 HTTP-заявок от двух пользователей стартовали через общий барьер.")
        print("РЕЗУЛЬТАТ: 1 бронь / 19 отказов «слот занят».")
        self.report["race"] = {"requests": clients, **dict(counts)}

        self.stage("4. Повтор запроса")
        repeated = self.post(winner, winning_key, command)
        require(repeated == (200, booking), "Повтор изменил результат бронирования.")
        print(f"Тот же request_id → та же бронь #{reservation_id}.")
        print("Новая запись брони не создаётся.")
        self.report["idempotent_retry"] = True

        self.stage("5. Права пользователя и отмена")
        other = "bob" if winner == "alice" else "alice"
        cancellation = {"op": "cancel", "reservation_id": reservation_id}
        forbidden, result = self.post(other, "wrong-cancel", cancellation)
        require(forbidden == 403 and result["code"] == "reservation_not_owner", "Чужая отмена не была запрещена.")
        print("Чужой токен → HTTP 403: отмена запрещена.")
        cancelled, result = self.post(winner, "owner-cancel", cancellation)
        require(cancelled == 200 and result["code"] == "cancelled", "Владелец не смог отменить бронь.")
        require(next(item for item in self.slots() if item["slot_id"] == "s1")["available"], "Слот не освободился.")
        print("Токен владельца → HTTP 200: слот освобождён.")
        status, new_booking = self.post(other, "book-after-cancel", command)
        require(status == 200 and new_booking["code"] == "reserved", "Освобождённый слот нельзя забронировать.")
        require(new_booking["reservation"]["reservation_id"] != reservation_id, "Повторно использован номер старой брони.")
        print(f"Новая заявка → бронь #{new_booking['reservation']['reservation_id']}.")
        self.report["foreign_cancel_status"] = forbidden
        self.report["owner_cancel_status"] = cancelled

        self.stage("6. Завершение процесса и восстановление")
        self.stop_server()  # Actual forced termination of this demo's own process.
        self.start_server()
        require(self.post(other, "book-after-cancel", command) == (200, new_booking), "После запуска изменился результат.")
        require(not next(item for item in self.slots() if item["slot_id"] == "s1")["available"], "Действующая бронь потеряна.")
        print("Процесс сервера принудительно завершён и запущен заново.")
        print(f"Бронь #{new_booking['reservation']['reservation_id']} и результат повтора восстановлены из журнала.")
        self.stop_server()
        check = Database.check(self.data)
        require(check["status"] == "ok", "Проверка журнала завершилась ошибкой.")
        require(check["state"]["reservations"] == 2 and check["state"]["active_reservations"] == 1
                and check["state"]["cancelled_reservations"] == 1, "История броней не согласована.")
        self.report["recovery"] = check["state"]
        self.report["journal_check"] = "ok"
        (self.directory / "report.json").write_text(json.dumps(self.report, indent=2), encoding="utf-8")

        print("\nВСЕ ПРОВЕРКИ ДЕМО ПРОЙДЕНЫ.")
        print("Компромиссы: один обработчик; индексы в памяти; журнал растёт.")
        print(f"Данные и отчёт сохранены: {self.directory}")


def main() -> int:
    # Keep Russian captions readable both in the Windows console and in pipes.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--auto", action="store_true", help="Run without Enter pauses for rehearsal/testing")
    parser.add_argument("--output-root", type=Path, default=PROJECT / "data" / "video-demo")
    args = parser.parse_args()
    demo = None
    try:
        demo = Demo(args.output_root, args.auto)
        demo.run()
        return 0
    except (EOFError, KeyboardInterrupt):
        print("\nДемо прервано.")
        return 130
    except (OSError, RuntimeError, ValueError, http.client.HTTPException) as exc:
        print(f"Демо не завершено: {exc}", file=sys.stderr)
        return 1
    finally:
        if demo is not None:
            demo.stop_server()


if __name__ == "__main__":
    raise SystemExit(main())
