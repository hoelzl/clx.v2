import asyncio
from abc import ABC, abstractmethod
from typing import Any, Iterable


class Operation(ABC):
    @abstractmethod
    async def exec(self) -> Any: ...


class NoOperation(Operation):
    async def exec(self) -> Any:
        pass


class Sequential(Operation):
    def __init__(self, operations: list[Operation]):
        self.operations = operations

    async def exec(self) -> Any:
        for operation in self.operations:
            await operation.exec()


class Concurrently(Operation):
    def __init__(self, operations: Iterable[Operation]):
        self.operations = list(operations)

    async def exec(self) -> Any:
        await asyncio.gather(*[operation.exec() for operation in self.operations])
