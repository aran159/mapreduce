from driver_pb2 import assignedTask


TMP_DIR = '.tmp/'
INTERMEDIATE_DIR = TMP_DIR + 'intermediate/'
OUT_DIR = TMP_DIR + 'out/'
MAP_INPUT_DIR = INTERMEDIATE_DIR + 'map_input/'
REDUCE_INPUT_DIR = INTERMEDIATE_DIR + 'reduce_input/'
MAP_INPUT_FILE_PREFIX = 'm'
REDUCE_INPUT_FILE_PREFIX = 'mr'
OUT_FILE_PREFIX = 'out'

DRIVER_PORT = 50051

TASK_NOT_ASSIGNED = assignedTask(id=-1, taskType=-1)
ALL_TASKS_COMPLETED = assignedTask(id=-2, taskType=-2)
WORKER_SLEEP_TIME_IN_SECONDS = 2
