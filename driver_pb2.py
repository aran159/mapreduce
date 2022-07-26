# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: driver.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import common_pb2 as common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x64river.proto\x1a\x0c\x63ommon.proto\"\x1e\n\x11healthCheckResult\x12\t\n\x01M\x18\x01 \x01(\x05\",\n\x0c\x61ssignedTask\x12\x10\n\x08taskType\x18\x01 \x01(\x05\x12\n\n\x02id\x18\x02 \x01(\x05\"F\n\x12\x61ssignedTaskResult\x12\x10\n\x08taskType\x18\x01 \x01(\x05\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x12\n\nstatusCode\x18\x03 \x01(\x05\x32\x9a\x01\n\x06\x44river\x12+\n\x0bhealthCheck\x12\x06.empty\x1a\x12.healthCheckResult\"\x00\x12\x30\n\x15requestTaskAssignment\x12\x06.empty\x1a\r.assignedTask\"\x00\x12\x31\n\x10notifyTaskStatus\x12\x13.assignedTaskResult\x1a\x06.empty\"\x00\x62\x06proto3')



_HEALTHCHECKRESULT = DESCRIPTOR.message_types_by_name['healthCheckResult']
_ASSIGNEDTASK = DESCRIPTOR.message_types_by_name['assignedTask']
_ASSIGNEDTASKRESULT = DESCRIPTOR.message_types_by_name['assignedTaskResult']
healthCheckResult = _reflection.GeneratedProtocolMessageType('healthCheckResult', (_message.Message,), {
  'DESCRIPTOR' : _HEALTHCHECKRESULT,
  '__module__' : 'driver_pb2'
  # @@protoc_insertion_point(class_scope:healthCheckResult)
  })
_sym_db.RegisterMessage(healthCheckResult)

assignedTask = _reflection.GeneratedProtocolMessageType('assignedTask', (_message.Message,), {
  'DESCRIPTOR' : _ASSIGNEDTASK,
  '__module__' : 'driver_pb2'
  # @@protoc_insertion_point(class_scope:assignedTask)
  })
_sym_db.RegisterMessage(assignedTask)

assignedTaskResult = _reflection.GeneratedProtocolMessageType('assignedTaskResult', (_message.Message,), {
  'DESCRIPTOR' : _ASSIGNEDTASKRESULT,
  '__module__' : 'driver_pb2'
  # @@protoc_insertion_point(class_scope:assignedTaskResult)
  })
_sym_db.RegisterMessage(assignedTaskResult)

_DRIVER = DESCRIPTOR.services_by_name['Driver']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _HEALTHCHECKRESULT._serialized_start=30
  _HEALTHCHECKRESULT._serialized_end=60
  _ASSIGNEDTASK._serialized_start=62
  _ASSIGNEDTASK._serialized_end=106
  _ASSIGNEDTASKRESULT._serialized_start=108
  _ASSIGNEDTASKRESULT._serialized_end=178
  _DRIVER._serialized_start=181
  _DRIVER._serialized_end=335
# @@protoc_insertion_point(module_scope)
