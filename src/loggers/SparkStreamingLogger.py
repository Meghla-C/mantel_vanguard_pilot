from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent


class SparkStreamingLogger(StreamingQueryListener):
    def __init__(self, logger):
        self.logger = logger
        super().__init__()
    
    def onQueryStarted(self, event: QueryStartedEvent):
        self.logger.info(f"Query started: {event.name} ({event.id})")

    def onQueryProgress(self, event: QueryProgressEvent):
        self.logger.info(f"Query progress: {event.progress.json}")

    def onQueryTerminated(self, event: QueryTerminatedEvent):
        if event.exception:
            self.logger.error(f"Query terminated: {event.id} ({event.exception})")
        else:
            self.logger.info(f"Query terminated: {event.id}")
