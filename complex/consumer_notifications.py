"""Consumidor de notificações: 'envia' e-mails e SMS.

Recebe mensagens publicadas tanto pelo producer_orders quanto pelo
producer_payments — demonstra dois produtores escrevendo na mesma fila.
"""
import json
import time

import rabbitpy

from const import AMQP_URL, Q_NOTIFICATIONS


def process(notif):
    ch = notif['channel'].upper()
    print(f'[notif] [{ch}] -> {notif["to"]}: {notif["subject"]}')
    time.sleep(0.25)
    print(f'[notif]        "{notif["body"]}"')


def main():
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            channel.prefetch_count(1)
            queue = rabbitpy.Queue(channel, Q_NOTIFICATIONS, durable=True, auto_delete=False)
            print(f'[notif] aguardando mensagens em "{Q_NOTIFICATIONS}"...')
            for message in queue.consume():
                notif = json.loads(message.body)
                process(notif)
                message.ack()


if __name__ == '__main__':
    main()
