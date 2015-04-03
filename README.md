# PoC comsumer for CERN's alarm notification infrastructure

See https://github.com/gmccance/GNI for general desceiption of infrastructure.

Current implementation is using a partitioned ActiveMQ message queue:

  * state: open     - single event latch-up on new alarm open
  * state: active   - multiple events, send repeatadly for open alarms
  * state: closed   - single event latch-down on alarm close

See http://itmon.web.cern.ch/itmon/ for specification.

This PoC is to show how to connect to the partitioned broker and consume alarm events
from the system.
