from typing import TYPE_CHECKING, Any, Optional

from faststream.message.utils import encode_message

if TYPE_CHECKING:
    from nats.js.kv import KeyValue

    from faststream._internal.basic_types import (
        SendableMessage,
    )


class KeyValueFacade:
    def __init__(self, key_value: "KeyValue") -> None:
        self.key_value = key_value

    async def get(self, key: str, revision: Optional[int] = None) -> "KeyValue.Entry":
        return await self.key_value.get(key=key, revision=revision)

    async def put(self, key: str, value: "SendableMessage") -> int:
        payload, _ = encode_message(value)
        return await self.key_value.put(key=key, value=payload)

    async def create(self, key: str, value: "SendableMessage") -> int:
        payload, _ = encode_message(value)
        return await self.key_value.create(key=key, value=payload)

    async def update(
        self, key: str, value: "SendableMessage", last: Optional[int] = None
    ) -> int:
        payload, _ = encode_message(value)
        return await self.key_value.update(key=key, value=payload, last=last)

    async def delete(self, key: str, last: Optional[int] = None) -> bool:
        return await self.key_value.delete(key=key, last=last)

    async def purge(self, key: str) -> bool:
        return await self.key_value.purge(key=key)

    async def purge_deletes(self, olderthan: int = 30 * 60) -> bool:
        return await self.key_value.purge_deletes(olderthan=olderthan)

    async def status(self) -> "KeyValue.BucketStatus":
        return await self.key_value.status

    async def watchall(self, **kwargs: Any) -> "KeyValue.KeyWatcher":
        return await self.key_value.watchall(**kwargs)

    async def history(self, key: str) -> list["KeyValue.Entry"]:
        return await self.key_value.history(key=key)

    async def watch(
        self,
        keys: list[str],
        headers_only: bool = False,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        inactive_threshold: Optional[float] = None,
    ) -> "KeyValue.KeyWatcher":
        await self.key_value.watch(
            keys=keys,
            headers_only=headers_only,
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            inactive_threshold=inactive_threshold,
        )
