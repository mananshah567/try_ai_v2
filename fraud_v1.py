# === File: fraud_langgraph_pipeline.py ===

from langgraph.graph import StateGraph
from langchain_core.runnables import RunnableLambda
from datetime import datetime
import random
import openai
import sqlalchemy
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, Boolean, DateTime

# === PostgreSQL Setup ===
DB_URL = "postgresql://user:password@localhost:5432/fraud_detection_db"  # Replace with your credentials
db_engine = create_engine(DB_URL)
db_metadata = MetaData()

transactions_table = Table("transactions", db_metadata,
    Column("transaction_id", String, primary_key=True),
    Column("timestamp", DateTime),
    Column("amount", Float),
    Column("location", String),
    Column("device_type", String),
    Column("merchant_category", String),
    Column("is_suspicious", Boolean),
    Column("explanation", String)
)
db_metadata.create_all(db_engine)

# === OpenAI Key ===
openai.api_key = "sk-..."  # Replace with your OpenAI key

# === Function: Rule-based Fraud Check ===
def rule_based_check(txn):
    return {
        "is_suspicious": (
            txn["amount"] > 500 or
            txn["location"] in ["RU", "NG"] or
            txn["merchant_category"] in ["luxury", "gambling"] or
            txn["device_type"] == "unknown"
        )
    }

# === Function: LLM-based Explanation ===
def explain_fraud_risk(txn):
    try:
        functions = [
            {
                "name": "explain_fraud_risk",
                "description": "Explain why a transaction might be flagged as suspicious",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "amount": {"type": "number"},
                        "location": {"type": "string"},
                        "device_type": {"type": "string"},
                        "merchant_category": {"type": "string"},
                    },
                    "required": ["amount", "location", "device_type", "merchant_category"]
                },
            }
        ]

        completion = openai.ChatCompletion.create(
            model="gpt-4-0613",
            messages=[
                {"role": "system", "content": "You are a fraud analyst."},
                {"role": "user", "content": f"Explain why this transaction may be suspicious: {txn}"},
            ],
            functions=functions,
            function_call="auto",
        )

        args = eval(completion.choices[0].message.function_call.arguments)
        reasons = []
        if args["location"] in ["RU", "NG"]:
            reasons.append(f"originated from high-risk country: {args['location']}")
        if args["amount"] > 500:
            reasons.append(f"large amount: ₹{args['amount']}")
        if args["device_type"] == "unknown":
            reasons.append("unknown device")
        if args["merchant_category"] in ["luxury", "gambling"]:
            reasons.append(f"risky merchant: {args['merchant_category']}")
        return {"explanation": ", ".join(reasons)}
    except Exception as e:
        return {"explanation": f"[Error generating explanation] {str(e)}"}

# === Function: Write to PostgreSQL ===
def write_to_postgres(txn):
    with db_engine.begin() as conn:
        conn.execute(transactions_table.insert().values(
            transaction_id=txn["transaction_id"],
            timestamp=txn["timestamp"],
            amount=txn["amount"],
            location=txn["location"],
            device_type=txn["device_type"],
            merchant_category=txn["merchant_category"],
            is_suspicious=txn.get("is_suspicious", False),
            explanation=txn.get("explanation")
        ))
    return {}

# === Function: Notify if Needed ===
def notify_if_needed(txn):
    if txn.get("is_suspicious"):
        print(f"\n[ALERT] 🚨 Suspicious transaction {txn['transaction_id']}\nExplanation: {txn.get('explanation')}\n")
    return {}

# === LangGraph Pipeline ===
from langgraph.graph import StateGraph
from langchain_core.runnables import RunnableLambda

def create_pipeline():
    graph = StateGraph()

    graph.add_node("fraud_check", RunnableLambda(rule_based_check))
    graph.add_node("explain", RunnableLambda(explain_fraud_risk))
    graph.add_node("log_to_db", RunnableLambda(write_to_postgres))
    graph.add_node("notify", RunnableLambda(notify_if_needed))

    def route(state):
        if state.get("is_suspicious"):
            return "explain"
        return "log_to_db"

    graph.set_entry_point("fraud_check")
    graph.add_conditional_edges("fraud_check", route)
    graph.add_edge("explain", "log_to_db")
    graph.add_edge("log_to_db", "notify")
    graph.set_finish_point("notify")

    return graph.compile()

# === Run with Streaming Test ===
if __name__ == "__main__":
    import asyncio
    import uuid

    pipeline = create_pipeline()

    async def simulate_stream():
        for i in range(5):
            txn = {
                "transaction_id": str(uuid.uuid4()),
                "timestamp": datetime.now(),
                "amount": random.choice([120, 800, 50, 999]),
                "location": random.choice(["IN", "RU", "US", "NG"]),
                "device_type": random.choice(["mobile", "unknown", "desktop"]),
                "merchant_category": random.choice(["groceries", "luxury", "gambling"]),
            }
            result = pipeline.invoke(txn)
            await asyncio.sleep(0.5)

    asyncio.run(simulate_stream())
