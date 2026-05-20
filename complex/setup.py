"""Declara o exchange e as 4 filas usadas pelo sistema de e-commerce.

Executar uma vez antes de subir produtores e consumidores.
"""
import rabbitpy

from const import (
    AMQP_URL, EXCHANGE,
    Q_ORDERS, Q_PAYMENTS, Q_INVENTORY, Q_NOTIFICATIONS,
    RK_ORDERS, RK_PAYMENTS, RK_INVENTORY, RK_NOTIFICATIONS,
)

QUEUE_BINDINGS = [
    (Q_ORDERS,        RK_ORDERS),
    (Q_PAYMENTS,      RK_PAYMENTS),
    (Q_INVENTORY,     RK_INVENTORY),
    (Q_NOTIFICATIONS, RK_NOTIFICATIONS),
]


def main():
    with rabbitpy.Connection(AMQP_URL) as conn:
        with conn.channel() as channel:
            exchange = rabbitpy.Exchange(
                channel, EXCHANGE, exchange_type='topic',
                durable=True,
            )
            exchange.declare()

            for qname, rkey in QUEUE_BINDINGS:
                q = rabbitpy.Queue(channel, qname, durable=True, auto_delete=False)
                q.declare()
                q.bind(exchange, rkey)
                print(f'[setup] fila "{qname}" criada e ligada a "{rkey}"')

            print('[setup] pronto.')


if __name__ == '__main__':
    main()
