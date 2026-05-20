"""Produtor de pedidos.

Simula o front-end da loja registrando novos pedidos de clientes.
Publica em duas filas: q_orders (para processamento) e q_notifications
(para confirmar o pedido para o cliente).
"""
import json
import random
import time
import uuid

import rabbitpy

from const import (
    AMQP_URL, EXCHANGE,
    RK_ORDERS, RK_NOTIFICATIONS,
)

CATALOG = [
    ('SKU-1001', 'Teclado Mecânico',      349.90),
    ('SKU-1002', 'Mouse Gamer',            199.90),
    ('SKU-1003', 'Monitor 27"',          1899.00),
    ('SKU-1004', 'Headset Bluetooth',     499.00),
    ('SKU-1005', 'Webcam Full HD',        289.50),
]

CUSTOMERS = [
    ('cust-01', 'Ana Silva',     'ana@example.com'),
    ('cust-02', 'Bruno Souza',   'bruno@example.com'),
    ('cust-03', 'Carla Lima',    'carla@example.com'),
    ('cust-04', 'Diego Pereira', 'diego@example.com'),
]


def build_order():
    sku, name, price = random.choice(CATALOG)
    qty = random.randint(1, 3)
    cust_id, cust_name, cust_email = random.choice(CUSTOMERS)
    return {
        'order_id':    f'ord-{uuid.uuid4().hex[:8]}',
        'customer_id': cust_id,
        'customer':    cust_name,
        'email':       cust_email,
        'item':        {'sku': sku, 'name': name, 'qty': qty, 'unit_price': price},
        'total':       round(price * qty, 2),
        'ts':          time.time(),
    }


def main(n=10, interval=1.0):
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE, exchange_type='topic', durable=True)

            for _ in range(n):
                order = build_order()

                rabbitpy.Message(channel, json.dumps(order)).publish(exchange, RK_ORDERS)

                notif = {
                    'channel': 'email',
                    'to':      order['email'],
                    'subject': f'Pedido {order["order_id"]} recebido',
                    'body':    f'Olá {order["customer"]}, recebemos seu pedido de '
                               f'{order["item"]["qty"]}x {order["item"]["name"]}.',
                    'ts':      time.time(),
                }
                rabbitpy.Message(channel, json.dumps(notif)).publish(exchange, RK_NOTIFICATIONS)

                print(f'[orders] publicado {order["order_id"]} '
                      f'({order["item"]["qty"]}x {order["item"]["name"]}) - R$ {order["total"]:.2f}')
                time.sleep(interval)


if __name__ == '__main__':
    main()
