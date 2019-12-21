import logging

import pika


class LoggingMixin:
    def __init__(self):
        self.__log: logging.Logger = logging.root.getChild(self.__class__.__module__ + '.' + self.__class__.__name__)

    @property
    def log(self) -> logging.Logger:
        return self.__log


def create_amqp_connection_from_url(url: str) -> pika.adapters.BlockingConnection:
    """
    Cria conexão AMQP via url

    :param url: URL de conexão
    """

    return pika.BlockingConnection(pika.URLParameters(url))
