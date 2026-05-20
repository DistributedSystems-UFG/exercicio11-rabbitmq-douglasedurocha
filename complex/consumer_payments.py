"""Consumidor de pagamentos: faz a conciliação com o gateway bancário."""
import json
import time

import rabbitpy

from const import AMQP_URL, Q_PAYMENTS


def process(pay):
    print(f'[pay-proc] conciliando {pay["payment_id"]} ({pay["method"]}, R$ {pay["amount"]:.2f})...')
    time.sleep(0.5)
    if pay['status'] == 'approved':
        print(f'[pay-proc]   -> aprovado. liberando pedido {pay["order_id"]} para expedição.')
    else:
        print(f'[pay-proc]   -> recusado. notificando cliente e cancelando reserva.')


def main():
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            channel.prefetch_count(1)
            queue = rabbitpy.Queue(channel, Q_PAYMENTS, durable=True, auto_delete=False)
            print(f'[pay-proc] aguardando mensagens em "{Q_PAYMENTS}"...')
            for message in queue.consume():
                pay = json.loads(message.body)
                process(pay)
                message.ack()


if __name__ == '__main__':
    main()
