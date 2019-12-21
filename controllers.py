import logging

import pika
import pika.exceptions
import pika.channel

# from pypelines.handlers import ConsumerHandler, ProducerHandler
from pypelines.utils import LoggingMixin
from pypelines.workers import BaseWorker, SimpleWorker


class BaseController(LoggingMixin):
    """
    Classe base para pipelines de dados com funcionamento através de mensageria com protocolo AMQP.

    O funcionamento do pipeline se dá a partir de um recebimento de mensagem processar a informação e repassar o que
    foi processado para frente, funcinando dessa forma como pipeline por streaming.


    Cada objeto terá duas conexões AMQP, uma para um publisher e outra para um subscriber, onde o modo dos exchanges são
    sempre fanout. Dessa forma é possível que vários outros pipelines se conectem com os dados gerados por um pipeline.

    Cada subclasse deve ser responsável por garantir o envio da mensagem no modo publisher e pelo recebimento e processamento
    no modo subscriber.
    """

    def __init__(self,
                 amqp_url: str,
                 input_exchange: str,
                 input_queue: str,
                 output_exchange: str = None,
                 input_exchange_type: str = 'fanout',
                 input_exchange_durable: bool = True,
                 input_queue_durable: bool = True,
                 input_queue_exclusive: bool = False,
                 input_channel_prefetch: int = 1,
                 input_auto_ack: bool = False,
                 output_exchange_type: str = 'fanout',
                 output_exchange_durable: bool = bool):
        super(BaseController, self).__init__()

        self._amqp_url: str = amqp_url
        self._input_exchange: str = input_exchange
        self._input_exchange_type: str = input_exchange_type
        self._input_exchange_durable: bool = input_exchange_durable
        self._input_queue: str = input_queue
        self._input_queue_durable: bool = input_queue_durable
        self._input_queue_exclusive: bool = input_queue_exclusive
        self._input_channel_prefetch: int = input_channel_prefetch
        self._input_auto_ack: bool = input_auto_ack
        self._output_exchange: str = output_exchange
        self._output_exchange_type: str = output_exchange_type
        self._output_exchange_durable: bool = output_exchange_durable

        # noinspection PyTypeChecker
        self._input_handler: ConsumerHandler = None
        # noinspection PyTypeChecker
        self._output_handler: ProducerHandler = None

        # noinspection PyTypeChecker
        # Objeto que possui a lógica de processamento da mensagem.
        self._worker: BaseWorker = None

    def run(self):
        # TODO Implementar uma forma de retry na classe que se falhar N vezes em X segundos a classe aborta o processo.
        while True:
            self.setup_input_handler()
            if self._output_exchange is not None:
                self.setup_output_handler()

            try:
                self._input_handler.run()
            except KeyboardInterrupt:
                self._input_handler.stop()
                self._input_handler.dispose()
                if self._output_handler is not None:
                    self._output_handler.dispose()
                break
            except pika.exceptions.ConnectionClosedByBroker:
                self.log.error(
                    'Connection of input handler closed by the message broker. Stopping execution of pipeline.'
                )
                break
            except pika.exceptions.AMQPChannelError as err:
                self.log.warning('Message broker channel error detected: {}. Retrying...'.format(err))

            except pika.exceptions.AMQPConnectionError as err:
                self.log.warning('Message broker connection error detected: {}. Retrying...'.format(err))

    def setup_input_handler(self):
        self._input_handler = ConsumerHandler(
            amqp_url=self._amqp_url,
            exchange=self._input_exchange,
            queue=self._input_queue,
            callback=self.message_callback,
            exchange_type=self._input_exchange_type,
            exchange_durable=self._input_exchange_durable,
            queue_exclusive=self._input_queue_exclusive,
            queue_durable=self._input_queue_durable,
            auto_ack=self._input_auto_ack
        )

    def setup_output_handler(self):
        self._output_handler = ProducerHandler(
            amqp_url=self._amqp_url,
            exchange=self._output_exchange,
            exchange_type=self._output_exchange_type,
            exchange_durable=self._output_exchange_durable
        )

    def setup_worker(self, worker: BaseWorker):
        self._worker = worker

    def message_callback(self,
                         channel: pika.channel.Channel,
                         method: pika.spec.Basic.Deliver,
                         properties: pika.spec.BasicProperties,
                         body: any) -> None:
        """
        Método responsável por controlar o fluxo de mensagens.

        input -> process -> output
        """
        raise NotImplementedError


class DirectController(BaseController):
    """
    Controller simples que recebe uma mensagem, processa e passa adiante.
    """

    def __init__(self, **kwargs):
        super(DirectController, self).__init__(**kwargs)

    def message_callback(self,
                         channel: pika.channel.Channel,
                         method: pika.spec.Basic.Deliver,
                         properties: pika.spec.BasicProperties,
                         body: any) -> None:
        try:
            msg = body.decode('utf-8')

            output = self._worker.process(msg)

            if self._output_handler is not None:
                self._output_handler.publish(body=output, properties=properties)

            channel.basic_ack(method.delivery_tag)

        except Exception as err:
            self.log.error('Não foi possível processar mensagem! Erro: %s' % err)
            self.log.exception(err)
            channel.basic_nack(method.delivery_tag)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname) -5s %(asctime) -5s %(name) -5s %(funcName) -5s: %(message)s')

    logging.getLogger('pika').setLevel(logging.WARNING)

    pipe = DirectController(amqp_url='amqp://mqadmin:Admin123XX_@192.168.6.144:5672/%2F',
                            input_exchange='exchange_test',
                            input_queue='queue_test')
    pipe.setup_worker(SimpleWorker())

    pipe.run()
