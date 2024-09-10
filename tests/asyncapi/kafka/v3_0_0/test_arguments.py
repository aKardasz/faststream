from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi.generate import get_app_schema
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):
    broker_factory = KafkaBroker

    def test_subscriber_bindings(self):
        broker = self.broker_factory()

        @broker.subscriber("test")
        async def handle(msg): ...

        schema = get_app_schema(self.build_app(broker), version="3.0.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "kafka": {"bindingVersion": "0.4.0", "topic": "test"}
        }
