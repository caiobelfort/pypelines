import pika
from pypelines.utils import LoggingMixin, create_amqp_connection_from_url
from pika.adapters.blocking_connection import (BlockingConnection, BlockingChannel)


class BaseHandler(LoggingMixin):

    def __int__(self):
        super(BaseHandler, self).__init__()


class AMQPHandler(BaseHandler):
    """
    Base AMQPHandler.

    Its maintain a blocking connection with an amqp server.
    """

    def __init__(self,
                 url: str,
                 prefetch_count=1
                 ):
        super(AMQPHandler, self).__init__()

        self._connection: BlockingConnection = create_amqp_connection_from_url(url)
        self._channel: BlockingChannel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=prefetch_count)

    def dispose(self):
        self._channel.close()
        self._connection.close()

    def __del__(self):
        self.dispose()

