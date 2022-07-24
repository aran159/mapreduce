from mapreduce.driver import Driver
from mapreduce.model import TaskStatus, TaskType, Tasks, Task


def test_initialize_task_list():
    N = 2
    M = 3
    driver = Driver('', N, M)
    driver.initialize_task_list()

    assert len(driver.tasks.filter(type=TaskType.MAP)) == N
    assert len(driver.tasks.filter(type=TaskType.REDUCE)) == M
    assert len(driver.tasks.filter()) == N + M
    assert len(driver.tasks.filter(status=TaskStatus.PENDING)) == N + M


def test_tasks_terminated():
    assert Tasks([
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.PENDING) for id in range(10)],
    ]).tasks_terminated is False

    assert Tasks([
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.PENDING) for id in range(10)],
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.IN_PROGRESS) for id in range(10)],
    ]).tasks_terminated is False

    assert Tasks([
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.DONE) for id in range(10)],
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.DONE) for id in range(10)],
        *[Task(id=id, type=TaskType.REDUCE, status=TaskStatus.DONE) for id in range(10)],
    ]).tasks_terminated is True

def test_map_tasks_terminated():
    assert Tasks([
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.IN_PROGRESS) for id in range(10)],
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.DONE) for id in range(10)],
        *[Task(id=id, type=TaskType.REDUCE, status=TaskStatus.PENDING) for id in range(10)],
    ]).map_tasks_terminated is False

    assert Tasks([
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.DONE) for id in range(10)],
        *[Task(id=id, type=TaskType.MAP, status=TaskStatus.DONE) for id in range(10)],
        *[Task(id=id, type=TaskType.REDUCE, status=TaskStatus.PENDING) for id in range(10)],
    ]).map_tasks_terminated is True
