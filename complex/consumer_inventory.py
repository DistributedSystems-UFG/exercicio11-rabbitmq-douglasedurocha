"""Consumidor de estoque: aplica deltas em um catálogo em memória."""
import json
import time

import rabbitpy

from const import AMQP_URL, Q_INVENTORY

stock = {f'SKU-100{i}': 100 for i in range(1, 6)}
blocked = set()


def process(ev):
    sku = ev['sku']
    if ev['event_type'] == 'block':
        blocked.add(sku)
        print(f'[inv-proc] {sku} BLOQUEADO em {ev["warehouse"]}.')
        return

    if sku in blocked and ev['event_type'] != 'restock':
        print(f'[inv-proc] {sku} está bloqueado, ignorando "{ev["event_type"]}".')
        return

    time.sleep(0.3)
    stock[sku] = max(0, stock.get(sku, 0) + ev['delta'])
    blocked.discard(sku)
    print(f'[inv-proc] {ev["warehouse"]} {sku} {ev["event_type"]} '
          f'({ev["delta"]:+d}) -> estoque atual: {stock[sku]}')


def main():
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            channel.prefetch_count(1)
            queue = rabbitpy.Queue(channel, Q_INVENTORY, durable=True, auto_delete=False)
            print(f'[inv-proc] aguardando mensagens em "{Q_INVENTORY}"...')
            for message in queue.consume():
                ev = json.loads(message.body)
                process(ev)
                message.ack()


if __name__ == '__main__':
    main()
