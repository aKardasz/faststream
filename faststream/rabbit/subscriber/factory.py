from typing import TYPE_CHECKING, Iterable, Optional, Union

from faststream.rabbit.schemas import RabbitExchange, RabbitQueue, ReplyConfig
from faststream.rabbit.subscriber.asyncapi import AsyncAPISubscriber

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from fast_depends.dependencies import Depends

    from faststream.broker.types import BrokerMiddleware
    from faststream.types import AnyDict


def create_subscriber(
    *,
    queue: RabbitQueue,
    exchange: Optional["RabbitExchange"],
    consume_args: Optional["AnyDict"],
    reply_config: Optional["ReplyConfig"],
    # Subscriber args
    no_ack: bool,
    no_reply: bool,
    retry: Union[bool, int],
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Iterable["BrokerMiddleware[IncomingMessage]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> AsyncAPISubscriber:
    return AsyncAPISubscriber(
        queue=queue,
        exchange=exchange,
        consume_args=consume_args,
        reply_config=reply_config,
        no_ack=no_ack,
        no_reply=no_reply,
        retry=retry,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )
