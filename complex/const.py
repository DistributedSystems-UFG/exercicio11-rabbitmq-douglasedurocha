"""Constantes compartilhadas entre todos os produtores e consumidores."""
from const import RABBITMQ_ADDR

AMQP_URL = f'amqp://myuser:abc123@{RABBITMQ_ADDR}:5672/my_vhost'

EXCHANGE = 'ecommerce'

Q_ORDERS = 'q_orders'
Q_PAYMENTS = 'q_payments'
Q_INVENTORY = 'q_inventory'
Q_NOTIFICATIONS = 'q_notifications'

RK_ORDERS = 'order.new'
RK_PAYMENTS = 'payment.received'
RK_INVENTORY = 'inventory.update'
RK_NOTIFICATIONS = 'notification.send'
