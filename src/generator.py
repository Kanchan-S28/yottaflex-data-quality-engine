import os
import random
from datetime import datetime, timedelta
import pandas as pd

def generate_data(output_dir="data/input", customers=100, products=30, orders=500,
                  seed=42, inject_issues=True):
    random.seed(seed)
    os.makedirs(output_dir, exist_ok=True)

    customer_rows = []
    for i in range(1, customers + 1):
        customer_rows.append({
            "customer_id": f"C{i:05d}",
            "customer_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "age": random.randint(18, 75),
            "created_at": (datetime.utcnow() - timedelta(days=random.randint(1, 1500))).isoformat()
        })
    product_rows = []
    categories = ["Electronics", "Home", "Fashion", "Grocery"]
    for i in range(1, products + 1):
        product_rows.append({
            "product_id": f"P{i:04d}",
            "product_name": f"Product {i}",
            "category": random.choice(categories),
            "unit_price": round(random.uniform(10, 1000), 2)
        })
    order_rows = []
    for i in range(1, orders + 1):
        p = random.choice(product_rows)
        qty = random.randint(1, 5)
        order_rows.append({
            "order_id": f"O{i:07d}",
            "customer_id": f"C{random.randint(1, customers):05d}",
            "product_id": p["product_id"],
            "quantity": qty,
            "amount": round(p["unit_price"] * qty, 2),
            "order_timestamp": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat()
        })

    if inject_issues and orders >= 10:
        order_rows[0]["quantity"] = -2
        order_rows[1]["customer_id"] = "C99999"  # invalid reference
        order_rows[2]["amount"] = -50
        order_rows[3]["order_id"] = order_rows[4]["order_id"]  # duplicate key
        order_rows[5]["order_timestamp"] = "not-a-date"

    pd.DataFrame(customer_rows).to_csv(f"{output_dir}/customers.csv", index=False)
    pd.DataFrame(product_rows).to_csv(f"{output_dir}/products.csv", index=False)
    pd.DataFrame(order_rows).to_csv(f"{output_dir}/orders.csv", index=False)

if __name__ == "__main__":
    generate_data(orders=1000)
