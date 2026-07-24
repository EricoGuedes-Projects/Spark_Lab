from faker import Faker
import random
import json
from datetime import datetime
from pathlib import Path

fake = Faker()

Path("data/raw").mkdir(
    parents=True,
    exist_ok=True
)

with open(
    "data/raw/transactions.json",
    "w",
    encoding="utf-8"
) as f:

    for i in range(10000):

        transaction = {
            "transaction_id": i,
            "user_id": random.randint(1, 500),
            "product_id": random.randint(1, 100),
            "amount": round(
                random.uniform(5, 5000),
                2
            ),
            "category": random.choice(
                [
                    "PIX",
                    "TED",
                    "COMPRA",
                    "SAQUE"
                ]
            ),
            "city": fake.city(),
            "status": random.choice(
                [
                    "APPROVED",
                    "FAILED",
                    "PENDING"
                ]
            ),
            "timestamp": datetime.now().isoformat()
        }

        f.write(
            json.dumps(transaction)
            + "\n"
        )

print("Dataset gerado.")