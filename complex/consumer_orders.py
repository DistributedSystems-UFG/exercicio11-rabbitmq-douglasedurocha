"""Consumidor de pedidos: valida e 'persiste' o pedido."""
import json
import time

import rabbitpy

from const import AMQP_URL, Q_ORDERS


def process(order):
    print(f'[order-proc] validando {order["order_id"]} de {order["customer"]}...')
    time.sleep(0.4)
    if order['total'] > 2000:
        print(f'[order-proc]   -> requer análise antifraude (total R$ {order["total"]:.2f})')
        time.sleep(0.6)
    print(f'[order-proc]   -> gravado no banco. status=AWAITING_PAYMENT')


def main():
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            channel.prefetch_count(1)
            queue = rabbitpy.Queue(channel, Q_ORDERS, durable=True, auto_delete=False)
            print(f'[order-proc] aguardando mensagens em "{Q_ORDERS}"...')
            for message in queue.consume():
                order = json.loads(message.body)
                process(order)
                message.ack()


if __name__ == '__main__':
    main()
