#step 2: une fois les données chargées depuis les csv et json, on créer les tables de la bdd
import sqlite3

conn= sqlite3.connect("data.db")
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS daily_city_sales (
    date TEXT NOT NULL,                     
    city TEXT,
    channel TEXT NOT NULL,
    orders_count INTEGER NOT NULL,
    unique_customers INTEGER NOT NULL,
    items_sold INTEGER NOT NULL,
    gross_revenue_eur REAL NOT NULL,
    refunds_eur REAL NOT NULL,
    net_revenue_eur REAL NOT NULL,
    PRIMARY KEY (date, city, channel)
);
                      
CREATE TABLE IF NOT EXISTS orders_clean (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                city TEXT,
                channel TEXT NOT NULL,
                items_sold INTEGER NOT NULL,
                created_at DATETIME,
                payment_status TEXT NOT NULL CHECK (payment_status = 'paid'),
                total_amount FLOAT
            );
                     
""")

conn.commit()
conn.close()


