"""Produtor de pagamentos.

Simula o gateway de pagamentos confirmando transações de pedidos.
Publica em q_payments (para conciliação) e q_notifications (recibo).
"""
import json
import random
import time
import uuid

import rabbitpy

from const import (
    AMQP_URL, EXCHANGE,
    RK_PAYMENTS, RK_NOTIFICATIONS,
)

METHODS = ['credit_card', 'pix', 'boleto']
STATUSES = ['approved', 'approved', 'approved', 'declined']


def build_payment():
    status = random.choice(STATUSES)
    return {
        'payment_id': f'pay-{uuid.uuid4().hex[:8]}',
        'order_id':   f'ord-{uuid.uuid4().hex[:8]}',
        'method':     random.choice(METHODS),
        'amount':     round(random.uniform(50.0, 2500.0), 2),
        'status':     status,
        'ts':         time.time(),
    }


def main(n=10, interval=1.3):
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE, exchange_type='topic', durable=True)

            for _ in range(n):
                pay = build_payment()
                rabbitpy.Message(channel, json.dumps(pay)).publish(exchange, RK_PAYMENTS)

                notif = {
                    'channel': 'sms',
                    'to':      '+55 62 9' + ''.join(random.choices('0123456789', k=8)),
                    'subject': 'Pagamento',
                    'body':    f'Pagamento {pay["payment_id"]} ({pay["method"]}) '
                               f'R$ {pay["amount"]:.2f}: {pay["status"]}.',
                    'ts':      time.time(),
                }
                rabbitpy.Message(channel, json.dumps(notif)).publish(exchange, RK_NOTIFICATIONS)

                print(f'[payments] {pay["payment_id"]} {pay["method"]} '
                      f'R$ {pay["amount"]:.2f} -> {pay["status"]}')
                time.sleep(interval)


if __name__ == '__main__':
    main()
