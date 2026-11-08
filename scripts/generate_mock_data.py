import argparse
import uuid
import random
from datetime import datetime, timezone, timedelta
from faker import Faker
import json
import os

# 1. Instance args analyser
parser = argparse.ArgumentParser(description="Generates fictional financial transactions in JSON Lines format to the Medallion pipeline")

# 2. Declaring accepted params
parser.add_argument("--records", type=int, default=1000, help= "Quantity of registers")
parser.add_argument("--output-dir", type=str, default="data/raw", help="Output directory")

# 3. Read args available in the run
args = parser.parse_args()

# print (f"Test {args.records} and {args.output_dir}")

fake = Faker ("en_US") # Generate data

# 4. Generating synthetic records 
def generate_record():
        return{
                "transaction_id": str(uuid.uuid4()),
                "customer_id": f"CUST_{random.randint(1,999)}",
                "customer_name": fake.name(),
                "amount": round(random.uniform(5.0, 50000.0),2),
                "currency": random.choice(["USD","EUR","BRL", "CAD", "usd", "eur", "brl", "cad"]),
                "channel": random.choice(["CREDIT_CARD", "DEBIT_CARD", "WIRE_TRANSFER", "ACH"]),
                "status": random.choices(["APPROVED", "DECLINED", "FRAUD_ALERT", "approved", "declined", "Fraud_Alert"], weights=[0.425,0.06,0.015,0.425,0.06,0.015])[0],
                "customer_job": fake.job(),
                "created_at": (datetime.now(timezone.utc) - timedelta(minutes=random.randint(0,1440))).isoformat()
        }


# 5. Saving the data
dict_list = []

for _ in range(args.records):
        dict_list.append(generate_record())
        

#TEST
# for item in dict_list:
#     print(item)



# 5.1 Checking dir existence
os.makedirs(args.output_dir, exist_ok=True)

#Vars for the writing
# Timestamp in UTC makes easier to international projects
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
file_name = f"data_{timestamp}.json"
file_path = os.path.join(args.output_dir, file_name)

# # 5.2 Open archive in writing mode ("w", encoding="utf-8")
with open(file_path, "w", encoding="utf-8") as f:
        for register in dict_list:
               f.write(json.dumps(register)+"\n")
                