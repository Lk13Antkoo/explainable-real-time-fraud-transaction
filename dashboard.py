import json
import requests
from kafka import KafkaConsumer
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich import box
from datetime import datetime
import csv
import os
import time
console = Console()

# Stats tracking
stats = {
    "total": 0,
    "normal": 0,
    "suspicious": 0,
    "highest_risk": 0.0,
    "total_amount": 0.0,
    "flagged_amount": 0.0
}

# Store last 15 transactions for the table
recent_transactions = []

def build_dashboard():
    # Top stats panels
    total_panel = Panel(
        f"[bold white]{stats['total']}[/bold white]",
        title="Total Scored",
        border_style="blue"
    )
    normal_panel = Panel(
        f"[bold green]{stats['normal']}[/bold green]",
        title="✅ Normal",
        border_style="green"
    )
    suspicious_panel = Panel(
        f"[bold red]{stats['suspicious']}[/bold red]",
        title="🚨 Suspicious",
        border_style="red"
    )
    risk_panel = Panel(
        f"[bold yellow]{stats['highest_risk']:.4f}[/bold yellow]",
        title="Highest Risk",
        border_style="yellow"
    )
    amount_panel = Panel(
        f"[bold magenta]${stats['flagged_amount']:.2f}[/bold magenta]",
        title="Flagged Amount",
        border_style="magenta"
    )

    columns = Columns([
        total_panel,
        normal_panel,
        suspicious_panel,
        risk_panel,
        amount_panel
    ])

    # Recent transactions table
    table = Table(
        title="Live Transaction Stream",
        box=box.ROUNDED,
        border_style="blue",
        header_style="bold cyan"
    )

    table.add_column("Time", style="dim", width=10)
    table.add_column("Amount", justify="right", width=12)
    table.add_column("Risk Score", justify="center", width=12)
    table.add_column("Status", justify="center", width=20)
    table.add_column("Review Feature", justify="left", width=25)
    table.add_column("Risk Bar", width=25)

    for txn in recent_transactions[-15:]:
        # Color based on risk
        if txn["flagged"]:
            status = "[bold red]🚨 SUSPICIOUS[/bold red]"
            amount_style = "red"
        else:
            status = "[bold green]✅ NORMAL[/bold green]"
            amount_style = "green"

        # Build a visual risk bar
        risk = txn["risk"]
        filled = int(risk * 20)
        empty = 20 - filled
        if risk > 0.8:
            bar_color = "red"
            feature_review = ", ".join(txn["top_features"][:5])  # Show top 5 features
        elif risk > 0.5:
            bar_color = "yellow"
            feature_review = ", ".join(txn["top_features"][:5])  # Show top 5 features

        else:
            bar_color = "green"
            feature_review = None
        bar = f"[{bar_color}]{'█' * filled}{'░' * empty}[/{bar_color}]"

        table.add_row(
            txn["time"],
            f"[{amount_style}]${txn['amount']:.2f}[/{amount_style}]",
            f"[bold]{txn['risk']:.4f}[/bold]",
            status,
            feature_review,
            bar
        )

    from rich.console import Group
    return Panel(
        Group(columns, table),
        title="[bold cyan]🔍 Fraud Detection Dashboard[/bold cyan]",
        border_style="cyan"
    )

# Connect to Kafka
consumer = KafkaConsumer(
    'transactions_raw',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

console.print("[bold cyan]Starting Fraud Detection Dashboard...[/bold cyan]")
console.print("[dim]Waiting for transactions...[/dim]\n")

with Live(build_dashboard(), refresh_per_second=2, screen=True) as live:
    for message in consumer:
        transaction = message.value
        transaction.pop('isFraud', None)

        # Call fraud detection API
        try:
            start = time.time()
            response = requests.post(
                'http://127.0.0.1:8000/score',
                json=transaction
            )
            end = time.time()
            inference_time = end - start
            
            result = response.json()
            risk = result['risk_score']
            flagged = result['is_flagged']
            top_feature_names = result['top_feature_names']

            amount = transaction['TransactionAmt']

            # Update stats
            stats["total"] += 1
            stats["total_amount"] += amount
            if True:
                stats["suspicious"] += 1
                stats["flagged_amount"] += amount
                
                # Save flagged transaction to CSV
                log_file = "fraud_log.csv"
                file_exists = os.path.exists(log_file)
                with open(log_file, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=[
                        "timestamp", "amount", "risk_score", "is_flagged", "inference_time"
                    ])
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow({
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "amount": round(amount, 2),
                        "risk_score": risk,
                        "is_flagged": flagged,
                        "inference_time": inference_time
                    })
            else:
                stats["normal"] += 1
            if risk > stats["highest_risk"]:
                stats["highest_risk"] = risk
            

            # Add to recent transactions
            recent_transactions.append({
                "time": datetime.now().strftime("%H:%M:%S"),
                "amount": amount,
                "risk": risk,
                "flagged": flagged,
                "top_features": top_feature_names
            })

            # Keep only last 15
            if len(recent_transactions) > 15:
                recent_transactions.pop(0)

            # Update dashboard
            live.update(build_dashboard())

        except Exception as e:
            pass