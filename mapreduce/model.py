from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple
from copy import deepcopy


class TaskType(Enum):
    MAP = 0
    REDUCE = 1


class TaskStatus(Enum):
    PENDING = 1
    IN_PROGRESS = 2
    DONE = 3


@dataclass
class Task:
    id: int
    type: TaskType
    status: TaskStatus = TaskStatus.PENDING


class Tasks:
    def __init__(self, tasks: List[Task] = []) -> Tasks:
        self._tasks = tasks

    def filter(self, *, type: TaskType = None, status: TaskStatus = None) -> Tasks:
        result = deepcopy(self._tasks)

        if type is not None:
            result = [task for task in result if task.type == type]

        if status is not None:
            result = [task for task in result if task.status == status]

        return Tasks(result)

    def set_status(self, task_type: TaskType, task_id: int, new_status: TaskStatus) -> None:
        index = self._find(task_type, task_id)
        self._tasks[index].status = new_status

    @property
    def tasks_terminated(self) -> bool:
        return (
            len(self)
            == len(self.filter(status=TaskStatus.DONE))
        )

    @property
    def map_tasks_terminated(self) -> bool:
        return (
            len(self.filter(type=TaskType.MAP))
            == len(self.filter(type=TaskType.MAP, status=TaskStatus.DONE))
        )

    def _find(self, task_type: TaskType, task_id: int) -> int:
        matches = [task for task in self._tasks if task.type == task_type and task.id == task_id]
        assert len(matches) == 1
        return self._tasks.index(matches[0])

    def __len__(self):
        return len(self._tasks)

    def __getitem__(self, index: int) -> Task:
        return self._tasks[index]
