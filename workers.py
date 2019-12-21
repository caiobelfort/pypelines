import pika
import pika.channel
import sqlalchemy
import sqlalchemy.pool

from pypelines.utils import LoggingMixin


class BaseWorker(LoggingMixin):
    """
    Worker básico que serve de classe base para os demais workers.


    Apenas printa a mensagem recebida na tela.

    """

    def __init__(self):
        super(BaseWorker, self).__init__()

    def process(self, message: any) -> any:
        raise NotImplementedError


class SimpleWorker(BaseWorker):

    def __init__(self):
        super(SimpleWorker, self).__init__()

    def process(self, message: any) -> any:
        self.log.debug('Received: %s' % message)
        return message


class DatabaseWorker(BaseWorker):
    """
    Worker especializado para executar operações em banco de dados.
    """

    def __init__(self,
                 sqlalchemy_url: str,
                 ):
        super(DatabaseWorker, self).__init__()

        self.engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(sqlalchemy_url)

        self.log.info('Engine de conexão criada: %s' % self.engine)

    def process(self, message: any) -> any:
        raise NotImplementedError
