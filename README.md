# Tbilisi Bus Ticket Control API

This FastAPI application simulates a **bus ticket control system** for Tbilisi. It allows passengers to make transactions, tracks them in a small database for **1-minute free commute**, and then moves them to a main database after the timer expires.

---

## Features

- Accept bus or train transactions via `/bus_transaction` endpoint.
- Transactions are first written to a **small database** after a 10-second delay.
- Each transaction stays in the small database for **60 seconds**:
  - During this time, the passenger can make multiple transactions with the **same card**, which updates the existing record.
- After 60 seconds, the transaction moves to the **main database**.
- `/ticket_control` endpoint returns the **current status** of a card from the small database.
- `status` has a **90% chance of success**.
- `time_left` shows remaining seconds in the free 1-minute window.

---
