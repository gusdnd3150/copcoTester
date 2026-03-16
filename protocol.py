"""
Atlas Copco Open Protocol 메시지 정의 및 빌더
Spec: OpenProtocol_Specification_R_2_14_0_9836 4415 01

Header: 20 bytes ASCII
  Byte 1-4   : Message length (NUL 제외)
  Byte 5-8   : MID
  Byte 9-11  : Revision
  Byte 12    : No Ack flag
  Byte 13-14 : Station ID
  Byte 15-16 : Spindle ID
  Byte 17-18 : Sequence number
  Byte 19    : Messaging interface
  Byte 20    : Spare

Message = Header(20) + Data + NUL(0x00)
"""

from datetime import datetime

HEADER_SIZE = 20

# ─── MID 이름 (Spec 기준) ────────────────────────────────────────────────────
MID_NAMES = {
    # Application Link Communication
    9997: "Communication acknowledge",
    9998: "Communication acknowledge error",
    # Application Communication
    1:    "Application Communication start",
    2:    "Application Communication start acknowledge",
    3:    "Application Communication stop",
    4:    "Application Communication negative acknowledge",
    5:    "Application Communication positive acknowledge",
    6:    "Application data message request",
    8:    "Application data message subscription",
    9:    "Application data message unsubscribe",
    # Parameter Set
    10:   "Parameter set ID upload request",
    11:   "Parameter set ID upload reply",
    12:   "Parameter set data upload request",
    13:   "Parameter set data upload reply",
    14:   "Parameter set selected subscribe",
    15:   "Parameter set selected",
    16:   "Parameter set selected acknowledge",
    17:   "Parameter set selected unsubscribe",
    18:   "Select parameter set",
    19:   "Set parameter set batch size",
    20:   "Reset parameter set batch counter",
    21:   "Lock at batch done subscribe",
    22:   "Lock at batch done upload",
    23:   "Lock at batch done upload acknowledge",
    24:   "Lock at batch done unsubscribe",
    25:   "Parameter user set download request",
    # Job
    30:   "Job ID upload request",
    31:   "Job ID upload reply",
    32:   "Job data upload request",
    33:   "Job data upload reply",
    34:   "Job info subscribe",
    35:   "Job info",
    36:   "Job info acknowledge",
    37:   "Job info unsubscribe",
    38:   "Select job",
    39:   "Job restart",
    # Tool
    40:   "Tool data upload request",
    41:   "Tool data upload reply",
    42:   "Disable tool",
    43:   "Enable tool",
    44:   "Disconnect tool request",
    45:   "Set calibration value request",
    46:   "Set primary tool request",
    47:   "Tool pairing handling",
    48:   "Tool pairing status",
    # VIN
    50:   "Vehicle ID number download request",
    51:   "Vehicle ID number subscribe",
    52:   "Vehicle ID number",
    53:   "Vehicle ID number acknowledge",
    54:   "Vehicle ID number unsubscribe",
    # Tightening Result
    60:   "Last tightening result data subscribe",
    61:   "Last tightening result data",
    62:   "Last tightening result data acknowledge",
    63:   "Last tightening result data unsubscribe",
    64:   "Old tightening result upload request",
    65:   "Old tightening result upload reply",
    66:   "Number of offline results",
    67:   "Tightening result list upload",
    # Alarm
    70:   "Alarm subscribe",
    71:   "Alarm",
    72:   "Alarm acknowledge",
    73:   "Alarm unsubscribe",
    74:   "Alarm acknowledged on controller",
    75:   "Alarm acknowledged on controller acknowledge",
    76:   "Alarm status",
    77:   "Alarm status acknowledge",
    78:   "Acknowledge alarm remotely on controller",
    # Time
    80:   "Read time upload request",
    81:   "Read time upload reply",
    82:   "Set time",
    # Multi-spindle
    90:   "Multi-spindle status subscribe",
    91:   "Multi-spindle status",
    92:   "Multi-spindle status acknowledge",
    93:   "Multi-spindle status unsubscribe",
    100:  "Multi-spindle result subscribe",
    # Keep Alive
    9999: "Keep alive",
}

# MID 0004 에러 코드 (Spec Table 20)
ERROR_CODES = {
    0:  "No Error",
    1:  "Invalid data",
    2:  "Parameter set ID not present",
    3:  "Parameter set can not be set",
    4:  "Parameter set not running",
    6:  "VIN upload subscription already exists",
    7:  "VIN upload subscription does not exist",
    8:  "VIN input source not granted",
    9:  "Last tightening result subscription already exists",
    10: "Last tightening result subscription does not exist",
    11: "Alarm subscription already exists",
    12: "Alarm subscription does not exist",
    13: "Parameter set selection subscription already exists",
    14: "Parameter set selection subscription does not exist",
    15: "Tightening ID requested not found",
    16: "Connection rejected protocol busy",
    97: "Internal fault",
    98: "Undefined command",
    99: "Invalid revision",
}


# ─── Message 클래스 ──────────────────────────────────────────────────────────

class Message:
    def __init__(self, mid: int, data: str = "", revision: int = 1,
                 no_ack: int = 0, station_id: int = 0, spindle_id: int = 0, seq: int = 0):
        self.mid = mid
        self.data = data
        self.revision = revision
        self.no_ack = no_ack
        self.station_id = station_id
        self.spindle_id = spindle_id
        self.seq = seq

    def to_bytes(self) -> bytes:
        length = HEADER_SIZE + len(self.data)
        header = (
            f"{length:04d}"
            f"{self.mid:04d}"
            f"{self.revision:03d}"
            f"{self.no_ack:01d}"
            f"{self.station_id:02d}"
            f"{self.spindle_id:02d}"
            f"{self.seq:02d}"
            f"0"
            f" "
        )
        assert len(header) == HEADER_SIZE
        return (header + self.data + '\x00').encode('latin-1')

    @classmethod
    def from_bytes(cls, raw: bytes) -> 'Message | None':
        try:
            text = raw.decode('latin-1').rstrip('\x00')
            if len(text) < HEADER_SIZE:
                return None
            length_field = text[0:4].strip()
            mid_field = text[4:8].strip()
            rev_field = text[8:11].strip()
            return cls(
                mid=int(mid_field) if mid_field.isdigit() else 0,
                data=text[HEADER_SIZE:],
                revision=int(rev_field) if rev_field.isdigit() else 1,
                no_ack=int(text[11]) if text[11].isdigit() else 0,
                station_id=int(text[12:14]) if text[12:14].isdigit() else 0,
                spindle_id=int(text[14:16]) if text[14:16].isdigit() else 0,
                seq=int(text[16:18]) if text[16:18].isdigit() else 0,
            )
        except Exception:
            return None

    @property
    def name(self) -> str:
        return MID_NAMES.get(self.mid, f"Unknown MID {self.mid:04d}")

    def __repr__(self):
        return f"Message(MID={self.mid:04d}, '{self.name}', data='{self.data[:40]}')"


# ─── 공통 ACK / NAK ──────────────────────────────────────────────────────────

def build_ack(mid: int) -> Message:
    """MID 0005 – Command accepted"""
    return Message(mid=5, data=f"{mid:04d}")


def build_nak(mid: int, error_code: int = 1) -> Message:
    """MID 0004 – Command negative acknowledge"""
    return Message(mid=4, data=f"{mid:04d}{error_code:02d}")


# ─── Application Communication ───────────────────────────────────────────────

def build_comm_start(revision: int = 1, keep_alive: int = 0) -> Message:
    """MID 0001 – Application Communication start

    Rev 1-6 : 데이터 없음 (헤더 20바이트만)
    Rev 7   : 01 + keep_alive(1)
              keep_alive: 0=킵어라이브 사용(필수), 1=킵어라이브 선택
    Example (Rev 3, no data): 00200001003         NUL
    """
    if revision >= 7:
        data = f"01{keep_alive:01d}"
    else:
        data = ""
    return Message(mid=1, data=data, revision=revision)


def build_comm_start_ack(cell_id: int = 1, channel_id: int = 1,
                          name: str = "PF6000",
                          revision: int = 1) -> Message:
    """MID 0002 – Application Communication start acknowledge

    Rev 1 (Spec Table 12, Example: 00570002            010001020103Airbag1...):
      01 + Cell ID(4) + 02 + Channel ID(2) + 03 + Name(25)
    """
    data = (
        f"01{cell_id:04d}"
        f"02{channel_id:02d}"
        f"03{name:<25.25}"
    )
    return Message(mid=2, data=data, revision=revision)


def build_comm_stop() -> Message:
    """MID 0003 – Application Communication stop"""
    return Message(mid=3)


def build_keep_alive() -> Message:
    """MID 9999 – Keep alive"""
    return Message(mid=9999)


# ─── Parameter Set ───────────────────────────────────────────────────────────

def build_ps_id_request() -> Message:
    """MID 0010 – Parameter set ID upload request"""
    return Message(mid=10)


def build_ps_id_reply(ps_ids: list[int]) -> Message:
    """MID 0011 – Parameter set ID upload reply"""
    data = f"{len(ps_ids):03d}" + "".join(f"{pid:03d}" for pid in ps_ids)
    return Message(mid=11, data=data)


def build_ps_data_request(ps_id: int) -> Message:
    """MID 0012 – Parameter set data upload request (Rev 1)"""
    return Message(mid=12, data=f"{ps_id:03d}")


def build_ps_data_reply(ps_id: int, name: str = "DEFAULT",
                         rotation: int = 1,      # 1=CW, 2=CCW
                         batch_size: int = 0,
                         torque_min: float = 0.0,
                         torque_max: float = 30.0,
                         torque_target: float = 20.0,
                         angle_min: int = 0,
                         angle_max: int = 9999,
                         angle_target: int = 0) -> Message:
    """MID 0013 – Parameter set data upload reply (Rev 1)"""
    data = (
        f"{ps_id:03d}"
        f"{name:<25.25}"
        f"{rotation:01d}"
        f"{batch_size:04d}"
        f"{int(torque_min * 100):06d}"
        f"{int((torque_min + torque_max) / 2 * 100):06d}"
        f"{int(torque_max * 100):06d}"
        f"{int(torque_target * 100):06d}"
        f"{angle_min:05d}"
        f"{angle_max:05d}"
        f"{angle_target:05d}"
        f"{0:06d}"
        f"{0:06d}"
        f"{0:04d}"
        f"{0:05d}"
    )
    return Message(mid=13, data=data)


def build_ps_selected_subscribe() -> Message:
    """MID 0014 – Parameter set selected subscribe"""
    return Message(mid=14)


def build_ps_selected_ack() -> Message:
    """MID 0016 – Parameter set selected acknowledge"""
    return Message(mid=16)


def build_ps_selected_unsubscribe() -> Message:
    """MID 0017 – Parameter set selected unsubscribe"""
    return Message(mid=17)


def build_select_ps(ps_id: int) -> Message:
    """MID 0018 – Select parameter set"""
    return Message(mid=18, data=f"{ps_id:03d}")


def build_ps_batch_size(batch_size: int) -> Message:
    """MID 0019 – Set parameter set batch size"""
    return Message(mid=19, data=f"{batch_size:04d}")


def build_reset_batch_counter() -> Message:
    """MID 0020 – Reset parameter set batch counter"""
    return Message(mid=20)


# ─── Job ─────────────────────────────────────────────────────────────────────

def build_job_id_request() -> Message:
    """MID 0030 – Job ID upload request"""
    return Message(mid=30)


def build_job_id_reply(job_ids: list[int]) -> Message:
    """MID 0031 – Job ID upload reply"""
    data = f"{len(job_ids):02d}" + "".join(f"{jid:02d}" for jid in job_ids)
    return Message(mid=31, data=data)


def build_job_data_request(job_id: int) -> Message:
    """MID 0032 – Job data upload request"""
    return Message(mid=32, data=f"{job_id:02d}")


def build_job_data_reply(job_id: int, name: str = "DEFAULT JOB",
                          ps_ids: list[int] | None = None) -> Message:
    """MID 0033 – Job data upload reply (Rev 1)"""
    if ps_ids is None:
        ps_ids = [1]
    ps_str = "".join(f"{pid:03d}{1:04d}{0:01d}" for pid in ps_ids)
    data = (
        f"{job_id:02d}"
        f"{name:<25.25}"
        f"{len(ps_ids):02d}"
        f"0"
        f"{0:04d}"
        f"0"
        f"0"
        f"0"
        f"0"
        f"0"
        f"{0:02d}"
        f"{len(ps_ids):02d}"
        f"{ps_str}"
    )
    return Message(mid=33, data=data)


def build_job_info_subscribe() -> Message:
    """MID 0034 – Job info subscribe"""
    return Message(mid=34)


def build_job_info_ack() -> Message:
    """MID 0036 – Job info acknowledge"""
    return Message(mid=36)


def build_job_info_unsubscribe() -> Message:
    """MID 0037 – Job info unsubscribe"""
    return Message(mid=37)


def build_select_job(job_id: int) -> Message:
    """MID 0038 – Select job"""
    return Message(mid=38, data=f"{job_id:02d}")


# ─── Tool ────────────────────────────────────────────────────────────────────

def build_tool_data_request() -> Message:
    """MID 0040 – Tool data upload request"""
    return Message(mid=40)


def build_tool_data_reply(tool_type: int = 12, serial: str = "PF60001234",
                           spindles: int = 1, motor_size: int = 62,
                           open_end: int = 0, controller_sn: str = "000000") -> Message:
    """MID 0041 – Tool data upload reply (Rev 1)"""
    data = (
        f"{tool_type:02d}"
        f"{serial:<14.14}"
        f"{spindles:02d}"
        f"{motor_size:03d}"
        f"{open_end:01d}"
        f"{controller_sn:<10.10}"
    )
    return Message(mid=41, data=data)


def build_disable_tool() -> Message:
    """MID 0042 – Disable tool"""
    return Message(mid=42)


def build_enable_tool() -> Message:
    """MID 0043 – Enable tool"""
    return Message(mid=43)


# ─── VIN ─────────────────────────────────────────────────────────────────────

def build_vin_download(vin: str) -> Message:
    """MID 0050 – Vehicle ID number download request"""
    return Message(mid=50, data=f"{vin:<25.25}")


def build_vin_subscribe() -> Message:
    """MID 0051 – Vehicle ID number subscribe"""
    return Message(mid=51)


def build_vin_ack() -> Message:
    """MID 0053 – Vehicle ID number acknowledge"""
    return Message(mid=53)


def build_vin_unsubscribe() -> Message:
    """MID 0054 – Vehicle ID number unsubscribe"""
    return Message(mid=54)


# ─── Tightening Result ───────────────────────────────────────────────────────

def build_subscribe_tightening() -> Message:
    """MID 0060 – Last tightening result data subscribe"""
    return Message(mid=60)


def build_tightening_result(
        cell_id: int = 1,
        channel_id: int = 1,
        controller_name: str = "PF6000",
        vin: str = "",
        job_id: int = 0,
        ps_id: int = 1,
        batch_size: int = 0,
        batch_counter: int = 0,
        status: int = 1,           # 0=NOK, 1=OK
        torque_status: int = 1,    # 0=Low, 1=OK, 2=High
        angle_status: int = 1,     # 0=Low, 1=OK, 2=High
        torque_min: float = 0.0,
        torque_max: float = 30.0,
        torque_target: float = 20.0,
        final_torque: float = 20.0,
        angle_min: int = 0,
        angle_max: int = 9999,
        angle_target: int = 0,
        final_angle: int = 0,
        batch_status: int = 1,     # 0=NOK, 1=OK, 2=Not used, 3=Running
        dt: datetime | None = None,
        tightening_id: int = 1) -> Message:
    """MID 0061 – Last tightening result data (Rev 1)

    Spec Table 96: 데이터에 필드 ID(01, 02, ...)가 포함됨
    Byte 위치는 1-based (헤더 포함)
    """
    if dt is None:
        dt = datetime.now()
    time_str = dt.strftime("%Y-%m-%d:%H:%M:%S")

    data = (
        f"01{cell_id:04d}"                   # bytes 21-26
        f"02{channel_id:02d}"                # bytes 27-30
        f"03{controller_name:<25.25}"        # bytes 31-57
        f"04{vin:<25.25}"                    # bytes 58-84
        f"05{job_id:02d}"                    # bytes 85-88
        f"06{ps_id:03d}"                     # bytes 89-93
        f"07{batch_size:04d}"                # bytes 94-99
        f"08{batch_counter:04d}"             # bytes 100-105
        f"09{status:01d}"                    # bytes 106-108
        f"10{torque_status:01d}"             # bytes 109-111
        f"11{angle_status:01d}"              # bytes 112-114
        f"12{int(torque_min * 100):06d}"     # bytes 115-122
        f"13{int(torque_max * 100):06d}"     # bytes 123-130
        f"14{int(torque_target * 100):06d}"  # bytes 131-138
        f"15{int(final_torque * 100):06d}"   # bytes 139-146
        f"16{angle_min:05d}"                 # bytes 147-153
        f"17{angle_max:05d}"                 # bytes 154-160
        f"18{angle_target:05d}"              # bytes 161-167
        f"19{final_angle:05d}"               # bytes 168-174
        f"20{time_str}"                      # bytes 175-195
        f"21{time_str}"                      # bytes 196-216
        f"22{batch_status:01d}"              # bytes 217-219
        f"23{tightening_id:010d}"            # bytes 220-231
    )
    return Message(mid=61, data=data)


def build_tightening_result_ack() -> Message:
    """MID 0062 – Last tightening result data acknowledge"""
    return Message(mid=62)


def build_unsubscribe_tightening() -> Message:
    """MID 0063 – Last tightening result data unsubscribe"""
    return Message(mid=63)


# ─── Alarm ───────────────────────────────────────────────────────────────────

def build_alarm_subscribe() -> Message:
    """MID 0070 – Alarm subscribe"""
    return Message(mid=70)


def build_alarm(error_code: str = "E001",
                ctrl_ready: int = 1,
                tool_ready: int = 1,
                dt: datetime | None = None) -> Message:
    """MID 0071 – Alarm (Rev 1)

    Spec Table 123:
      01 + error_code(4) + 02 + ctrl_ready(1) + 03 + tool_ready(1) + 04 + time(19)
    """
    if dt is None:
        dt = datetime.now()
    time_str = dt.strftime("%Y-%m-%d:%H:%M:%S")
    data = (
        f"01{error_code:<4.4}"
        f"02{ctrl_ready:01d}"
        f"03{tool_ready:01d}"
        f"04{time_str}"
    )
    return Message(mid=71, data=data)


def build_alarm_ack() -> Message:
    """MID 0072 – Alarm acknowledge (데이터 없음)"""
    return Message(mid=72)


def build_alarm_unsubscribe() -> Message:
    """MID 0073 – Alarm unsubscribe"""
    return Message(mid=73)


# ─── Time ────────────────────────────────────────────────────────────────────

def build_time_request() -> Message:
    """MID 0080 – Read time upload request"""
    return Message(mid=80)


def build_time_reply(dt: datetime | None = None) -> Message:
    """MID 0081 – Read time upload reply"""
    if dt is None:
        dt = datetime.now()
    return Message(mid=81, data=dt.strftime("%Y-%m-%d:%H:%M:%S"))


def build_set_time(dt: datetime | None = None) -> Message:
    """MID 0082 – Set time"""
    if dt is None:
        dt = datetime.now()
    return Message(mid=82, data=dt.strftime("%Y-%m-%d:%H:%M:%S"))