from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from pathlib import Path
import csv
import time
import threading
import random

app = FastAPI(title="Tbilisi Bus Ticket Control API",
              description="Import and check the data of the passengers transactions on buses and trains of Tbilisi",
              version="1.0.0")

# Paths for CSV files
small_database_path = Path("Tbilisi Bus Database/small_database.csv")
main_database_path = Path("Tbilisi Bus Database/main_database.csv")

# Ensure CSV files exist with headers
if not small_database_path.exists():
    small_database_path.parent.mkdir(parents=True, exist_ok=True)
    with open(small_database_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "passenger_name", "first_transaction_time", "card_number", "card_type",
            "transaction_ids", "status", "buses", "trains"
        ])

if not main_database_path.exists():
    with open(main_database_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "passenger_name", "transaction_timestamps", "card_number", "card_type",
            "transaction_ids", "status", "buses", "trains"
        ])


# Pydantic model for passenger transaction
class BusTransaction(BaseModel):
    passenger_name: str
    transaction_timestamp: str  # Single timestamp per request
    card_number: int
    card_type: str
    transaction_id: str
    buses: str
    trains: str


# Utility function
def random_status():
    return "success" if random.random() < 0.9 else "fail"


# Background task
def handle_transaction(transaction: BusTransaction):
    # Sleep 10 seconds before writing to small database
    time.sleep(10)

    rows = []
    with open(small_database_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0]
    data_rows = rows[1:]
    updated = False

    # Current timestamp for the first transaction time
    now = int(time.time())

    for r in data_rows:
        if r[2] == str(transaction.card_number):  # existing card_number
            # Append new data
            r[4] += f", {transaction.transaction_id}"
            r[6] += f", {transaction.buses}"
            r[7] += f", {transaction.trains}"
            r[5] = random_status()  # update status
            updated = True

    if not updated:
        # Add new transaction
        data_rows.append([
            transaction.passenger_name,
            str(now),  # first_transaction_time
            transaction.card_number,
            transaction.card_type,
            transaction.transaction_id,
            random_status(),
            transaction.buses,
            transaction.trains
        ])

    # Write updated small database
    with open(small_database_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data_rows)

    # Start countdown to move to main database
    threading.Thread(target=countdown_and_move, args=(transaction.card_number, now)).start()


def countdown_and_move(card_number, start_time):
    while True:
        time.sleep(1)
        # Calculate time passed
        elapsed = int(time.time()) - start_time
        if elapsed >= 60:
            # Move to main database
            move_to_main_database(card_number)
            break


def move_to_main_database(card_number):
    rows = []
    with open(small_database_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0]
    data_rows = rows[1:]
    new_data_rows = []

    for r in data_rows:
        if r[2] == str(card_number):
            # Append to main database (without first_transaction_time)
            with open(main_database_path, mode="a", newline="", encoding="utf-8") as f_main:
                writer = csv.writer(f_main)
                writer.writerow([
                    r[0], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(r[1]))),
                    r[2], r[3], r[4], r[5], r[6], r[7]
                ])
        else:
            new_data_rows.append(r)

    # Rewrite small database without moved row
    with open(small_database_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(new_data_rows)


@app.post("/bus_transaction")
async def bus_transaction(transaction: BusTransaction, background_tasks: BackgroundTasks):
    background_tasks.add_task(handle_transaction, transaction)
    return {"Message": "Your Transaction has been Received"}


@app.get("/ticket_control")
def ticket_control(card_number: int):
    now = int(time.time())
    with open(small_database_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Strip spaces to be safe
            row_card = row["card_number"].strip()
            if row_card.isdigit() and int(row_card) == card_number:
                first_time = int(row["first_transaction_time"].strip())
                time_left = max(0, 60 - (now - first_time))
                return {
                    "passenger_name": row["passenger_name"].strip(),
                    "status": row["status"].strip(),
                    "buses": row["buses"].strip(),
                    "trains": row["trains"].strip(),
                    "time_left": time_left
                }
    return {"Message": "No active transaction found for this card"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

