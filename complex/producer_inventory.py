"""Produtor de eventos de estoque.

Simula o WMS (sistema do depósito) emitindo atualizações de estoque:
reposição, ajuste de inventário ou bloqueio de SKU.
"""
import json
import random
import time

import rabbitpy

from const import AMQP_URL, EXCHANGE, RK_INVENTORY

SKUS = ['SKU-1001', 'SKU-1002', 'SKU-1003', 'SKU-1004', 'SKU-1005']
EVENT_TYPES = ['restock', 'adjust', 'block']


def build_event():
    etype = random.choice(EVENT_TYPES)
    sku = random.choice(SKUS)
    delta = random.randint(-5, 30) if etype == 'adjust' else random.randint(5, 50)
    return {
        'sku':        sku,
        'event_type': etype,
        'delta':      0 if etype == 'block' else delta,
        'warehouse':  random.choice(['GYN-01', 'BSB-02', 'SAO-03']),
        'ts':         time.time(),
    }


def main(n=10, interval=1.7):
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(channel, EXCHANGE, exchange_type='topic', durable=True)

            for _ in range(n):
                ev = build_event()
                rabbitpy.Message(channel, json.dumps(ev)).publish(exchange, RK_INVENTORY)
                print(f'[inventory] {ev["warehouse"]} {ev["sku"]} '
                      f'{ev["event_type"]} ({ev["delta"]:+d})')
                time.sleep(interval)


if __name__ == '__main__':
    main()
