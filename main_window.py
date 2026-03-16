"""
Atlas Copco PF6000 테스터 메인 윈도우
Spec: OpenProtocol_Specification_R_2_14_0_9836 4415 01

- Client 모드: 실제 PF6000에 연결하여 MID 송수신 (Integrator 역할)
- Server 모드: PF6000 시뮬레이터 (Controller 역할, 클라이언트 자동 응답)
"""

import socket as _socket
from datetime import datetime

from PySide6.QtCore import Qt, Slot, QTimer
from flow_layout import FlowLayout
from PySide6.QtGui import QColor, QFont, QTextCursor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QTextEdit, QTabWidget,
    QComboBox, QSpinBox, QDoubleSpinBox, QRadioButton, QButtonGroup,
    QSplitter, QListWidget, QListWidgetItem, QFormLayout,
    QStatusBar, QCheckBox, QScrollArea,
)

import protocol as proto
from network import TcpClient, TcpServer, ClientConnection


# ─── 로그 위젯 ────────────────────────────────────────────────────────────────

class LogWidget(QTextEdit):
    MAX_LINES = 2000

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setFont(QFont("Courier New", 9))
        self.setMinimumHeight(160)

    def _append(self, text: str, color: str, sub: str = ""):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self.moveCursor(QTextCursor.MoveOperation.End)
        self.setTextColor(QColor(color))
        self.insertPlainText(f"[{ts}] {text}\n")
        if sub:
            self.setTextColor(QColor("#999999"))
            self.insertPlainText(f"          {sub}\n")
        self.setTextColor(QColor("#000000"))
        self.ensureCursorVisible()
        doc = self.document()
        if doc.blockCount() > self.MAX_LINES:
            cursor = QTextCursor(doc)
            cursor.movePosition(QTextCursor.MoveOperation.Start)
            cursor.select(QTextCursor.SelectionType.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()

    def tx(self, msg: proto.Message):
        raw = msg.to_bytes()[:-1].decode('latin-1')   # NUL 제외
        summary = f"TX → [{msg.mid:04d}] {msg.name}  ({len(raw)} bytes)"
        self._append(summary, "#0055cc", sub=raw)

    def rx(self, msg: proto.Message):
        raw = msg.to_bytes()[:-1].decode('latin-1')   # NUL 제외
        summary = f"RX ← [{msg.mid:04d}] {msg.name}  ({len(raw)} bytes)"
        self._append(summary, "#006600", sub=raw)

    def info(self, text: str):
        self._append(f"[INFO] {text}", "#555555")

    def error(self, text: str):
        self._append(f"[ERROR] {text}", "#cc0000")

    def warn(self, text: str):
        self._append(f"[WARN] {text}", "#bb6600")


def _btn(text: str, callback=None) -> QPushButton:
    b = QPushButton(text)
    if callback:
        b.clicked.connect(callback)
    return b


# ─── 탭: 통신 (MID 0001 / 0002 / 0003 / 9999) ────────────────────────────────

class CommTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        # MID 0001 Communication Start  [Client 전용]
        self.grp_client = QGroupBox("MID 0001 – Application Communication start  [Integrator → Controller]")
        g1 = QFormLayout(self.grp_client)
        self.revision = QSpinBox(); self.revision.setRange(1, 7); self.revision.setValue(1)
        self.keep_alive = QComboBox()
        self.keep_alive.addItems(["0 – Use Keep alive (필수)", "1 – Ignore Keep alive (선택)"])
        self.keep_alive.setEnabled(False)
        self.revision.valueChanged.connect(
            lambda v: self.keep_alive.setEnabled(v >= 7))
        g1.addRow("Revision:", self.revision)
        g1.addRow("Keep alive (Rev 7↑):", self.keep_alive)
        g1.addRow(QLabel("※ Rev 1~6: 데이터 없음 (헤더 20 bytes만 전송)"))
        self.grp_client.layout().addRow(_btn("Send MID 0001", self._send_0001))
        layout.addWidget(self.grp_client)

        # MID 0002 Communication Start ACK  [Server 전용]
        self.grp_server = QGroupBox("MID 0002 – Application Communication start acknowledge  [Controller → Integrator]")
        g2 = QFormLayout(self.grp_server)
        self.ack_cell_id = QSpinBox(); self.ack_cell_id.setRange(0, 9999); self.ack_cell_id.setValue(1)
        self.ack_channel_id = QSpinBox(); self.ack_channel_id.setRange(0, 99); self.ack_channel_id.setValue(1)
        self.ack_name = QLineEdit("PF6000"); self.ack_name.setMaxLength(25)
        g2.addRow("Cell ID:", self.ack_cell_id)
        g2.addRow("Channel ID:", self.ack_channel_id)
        g2.addRow("Controller Name:", self.ack_name)
        g2.addRow(QLabel("※ 필드 ID 포함: 01+CellID(4) 02+ChID(2) 03+Name(25)"))
        self.grp_server.layout().addRow(_btn("Send MID 0002 (Broadcast)", self._send_0002))
        layout.addWidget(self.grp_server)

        grp3 = QGroupBox("기타 (공통)")
        g3 = QHBoxLayout(grp3)
        g3.addWidget(_btn("MID 0003 – Communication stop", self._send_0003))
        g3.addWidget(_btn("MID 9999 – Keep alive", lambda: self.w.send(proto.build_keep_alive())))
        layout.addWidget(grp3)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0001(self):
        rev = self.revision.value()
        ka = int(self.keep_alive.currentText()[0]) if rev >= 7 else 0
        self.w.send(proto.build_comm_start(revision=rev, keep_alive=ka))

    def _send_0002(self):
        self.w.send(proto.build_comm_start_ack(
            cell_id=self.ack_cell_id.value(),
            channel_id=self.ack_channel_id.value(),
            name=self.ack_name.text(),
        ))

    def _send_0003(self):
        self.w.send(proto.build_comm_stop())


# ─── 탭: Parameter Set (MID 0010~0020) ───────────────────────────────────────

class ParameterSetTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        # Integrator → Controller 요청  [Client 전용]
        self.grp_client = QGroupBox("Integrator → Controller 요청")
        grp_req = self.grp_client
        g_req = QFormLayout(grp_req)

        h_ps_upload = QHBoxLayout()
        h_ps_upload.addWidget(_btn("MID 0010 – PS ID 목록 요청", lambda: self.w.send(proto.build_ps_id_request())))
        g_req.addRow("PS ID 목록 업로드:", h_ps_upload)

        h_ps_data = QHBoxLayout()
        self.ps_id_req = QSpinBox(); self.ps_id_req.setRange(1, 999); self.ps_id_req.setValue(1)
        h_ps_data.addWidget(QLabel("PS ID:")); h_ps_data.addWidget(self.ps_id_req)
        h_ps_data.addWidget(_btn("MID 0012 – PS 데이터 요청", lambda: self.w.send(proto.build_ps_data_request(self.ps_id_req.value()))))
        g_req.addRow("PS 데이터 업로드:", h_ps_data)

        h_ps_sub = QHBoxLayout()
        h_ps_sub.addWidget(_btn("MID 0014 – PS Selected 구독", lambda: self.w.send(proto.build_ps_selected_subscribe())))
        h_ps_sub.addWidget(_btn("MID 0016 – PS Selected ACK", lambda: self.w.send(proto.build_ps_selected_ack())))
        h_ps_sub.addWidget(_btn("MID 0017 – PS Selected 구독해지", lambda: self.w.send(proto.build_ps_selected_unsubscribe())))
        g_req.addRow("PS Selected 구독:", h_ps_sub)

        h_ps_sel = QHBoxLayout()
        self.ps_id_sel = QSpinBox(); self.ps_id_sel.setRange(1, 999); self.ps_id_sel.setValue(1)
        h_ps_sel.addWidget(QLabel("PS ID:")); h_ps_sel.addWidget(self.ps_id_sel)
        h_ps_sel.addWidget(_btn("MID 0018 – PS 선택", lambda: self.w.send(proto.build_select_ps(self.ps_id_sel.value()))))
        g_req.addRow("PS 선택:", h_ps_sel)

        h_ps_batch = QHBoxLayout()
        self.ps_batch_size = QSpinBox(); self.ps_batch_size.setRange(0, 9999)
        h_ps_batch.addWidget(QLabel("Batch Size:")); h_ps_batch.addWidget(self.ps_batch_size)
        h_ps_batch.addWidget(_btn("MID 0019 – Batch Size 설정", lambda: self.w.send(proto.build_ps_batch_size(self.ps_batch_size.value()))))
        h_ps_batch.addWidget(_btn("MID 0020 – Batch Counter 리셋", lambda: self.w.send(proto.build_reset_batch_counter())))
        g_req.addRow("Batch 설정:", h_ps_batch)
        layout.addWidget(grp_req)

        # Controller → Integrator 응답 (서버 모드)  [Server 전용]
        self.grp_server = QGroupBox("Controller → Integrator 응답 설정 (서버 모드)")
        grp_reply = self.grp_server
        g_reply = QFormLayout(grp_reply)

        self.ps_ids_input = QLineEdit("1,2,3")
        g_reply.addRow("MID 0011 – PS ID 목록 (쉼표 구분):", self.ps_ids_input)
        grp_reply.layout().addRow(_btn("MID 0011 – PS ID 목록 전송 (Broadcast)", self._send_0011))

        self.r_ps_id = QSpinBox(); self.r_ps_id.setRange(1, 999); self.r_ps_id.setValue(1)
        self.r_ps_name = QLineEdit("TORQUE_20NM"); self.r_ps_name.setMaxLength(25)
        self.r_rotation = QComboBox(); self.r_rotation.addItems(["1 – CW", "2 – CCW"])
        self.r_batch = QSpinBox(); self.r_batch.setRange(0, 9999)
        self.r_torque_min = QDoubleSpinBox(); self.r_torque_min.setRange(0, 9999); self.r_torque_min.setValue(18.0); self.r_torque_min.setSuffix(" Nm")
        self.r_torque_max = QDoubleSpinBox(); self.r_torque_max.setRange(0, 9999); self.r_torque_max.setValue(22.0); self.r_torque_max.setSuffix(" Nm")
        self.r_torque_target = QDoubleSpinBox(); self.r_torque_target.setRange(0, 9999); self.r_torque_target.setValue(20.0); self.r_torque_target.setSuffix(" Nm")
        self.r_angle_min = QSpinBox(); self.r_angle_min.setRange(0, 99999); self.r_angle_min.setSuffix(" °")
        self.r_angle_max = QSpinBox(); self.r_angle_max.setRange(0, 99999); self.r_angle_max.setValue(9999); self.r_angle_max.setSuffix(" °")
        self.r_angle_target = QSpinBox(); self.r_angle_target.setRange(0, 99999); self.r_angle_target.setValue(180); self.r_angle_target.setSuffix(" °")

        g_reply.addRow("PS ID:", self.r_ps_id)
        g_reply.addRow("PS 이름:", self.r_ps_name)
        g_reply.addRow("회전 방향:", self.r_rotation)
        g_reply.addRow("Batch Size:", self.r_batch)
        g_reply.addRow("토크 최소:", self.r_torque_min)
        g_reply.addRow("토크 최대:", self.r_torque_max)
        g_reply.addRow("토크 타겟:", self.r_torque_target)
        g_reply.addRow("각도 최소:", self.r_angle_min)
        g_reply.addRow("각도 최대:", self.r_angle_max)
        g_reply.addRow("각도 타겟:", self.r_angle_target)
        grp_reply.layout().addRow(_btn("MID 0013 – PS 데이터 전송 (Broadcast)", self._send_0013))
        layout.addWidget(grp_reply)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0011(self):
        try:
            ids = [int(x.strip()) for x in self.ps_ids_input.text().split(',') if x.strip()]
            self.w.send(proto.build_ps_id_reply(ids))
        except ValueError:
            self.w.log.error("PS ID 목록 형식 오류 (예: 1,2,3)")

    def _send_0013(self):
        self.w.send(proto.build_ps_data_reply(
            ps_id=self.r_ps_id.value(),
            name=self.r_ps_name.text(),
            rotation=int(self.r_rotation.currentText()[0]),
            batch_size=self.r_batch.value(),
            torque_min=self.r_torque_min.value(),
            torque_max=self.r_torque_max.value(),
            torque_target=self.r_torque_target.value(),
            angle_min=self.r_angle_min.value(),
            angle_max=self.r_angle_max.value(),
            angle_target=self.r_angle_target.value(),
        ))


# ─── 탭: Job (MID 0030~0039) ─────────────────────────────────────────────────

class JobTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        self.grp_client = QGroupBox("Integrator → Controller 요청")
        grp_req = self.grp_client
        g_req = QFormLayout(grp_req)

        h_job_upload = QHBoxLayout()
        h_job_upload.addWidget(_btn("MID 0030 – Job ID 목록 요청", lambda: self.w.send(proto.build_job_id_request())))
        g_req.addRow("Job ID 목록:", h_job_upload)

        h_job_data = QHBoxLayout()
        self.job_id_req = QSpinBox(); self.job_id_req.setRange(0, 99); self.job_id_req.setValue(1)
        h_job_data.addWidget(QLabel("Job ID:")); h_job_data.addWidget(self.job_id_req)
        h_job_data.addWidget(_btn("MID 0032 – Job 데이터 요청", lambda: self.w.send(proto.build_job_data_request(self.job_id_req.value()))))
        g_req.addRow("Job 데이터 업로드:", h_job_data)

        h_job_sub = QHBoxLayout()
        h_job_sub.addWidget(_btn("MID 0034 – Job Info 구독", lambda: self.w.send(proto.build_job_info_subscribe())))
        h_job_sub.addWidget(_btn("MID 0036 – Job Info ACK", lambda: self.w.send(proto.build_job_info_ack())))
        h_job_sub.addWidget(_btn("MID 0037 – Job Info 구독해지", lambda: self.w.send(proto.build_job_info_unsubscribe())))
        g_req.addRow("Job Info 구독:", h_job_sub)

        h_job_sel = QHBoxLayout()
        self.job_id_sel = QSpinBox(); self.job_id_sel.setRange(0, 99); self.job_id_sel.setValue(1)
        h_job_sel.addWidget(QLabel("Job ID:")); h_job_sel.addWidget(self.job_id_sel)
        h_job_sel.addWidget(_btn("MID 0038 – Job 선택", lambda: self.w.send(proto.build_select_job(self.job_id_sel.value()))))
        g_req.addRow("Job 선택:", h_job_sel)
        layout.addWidget(grp_req)

        self.grp_server = QGroupBox("Controller → Integrator 응답 설정 (서버 모드)")
        grp_reply = self.grp_server
        g_reply = QFormLayout(grp_reply)
        self.job_ids_input = QLineEdit("1,2")
        g_reply.addRow("MID 0031 – Job ID 목록 (쉼표 구분):", self.job_ids_input)
        grp_reply.layout().addRow(_btn("MID 0031 – Job ID 목록 전송 (Broadcast)", self._send_0031))

        self.r_job_id = QSpinBox(); self.r_job_id.setRange(0, 99); self.r_job_id.setValue(1)
        self.r_job_name = QLineEdit("ASSEMBLY JOB 1"); self.r_job_name.setMaxLength(25)
        self.r_job_ps_ids = QLineEdit("1,2")
        g_reply.addRow("Job ID:", self.r_job_id)
        g_reply.addRow("Job 이름:", self.r_job_name)
        g_reply.addRow("PS ID 목록 (쉼표 구분):", self.r_job_ps_ids)
        grp_reply.layout().addRow(_btn("MID 0033 – Job 데이터 전송 (Broadcast)", self._send_0033))
        layout.addWidget(grp_reply)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0031(self):
        try:
            ids = [int(x.strip()) for x in self.job_ids_input.text().split(',') if x.strip()]
            self.w.send(proto.build_job_id_reply(ids))
        except ValueError:
            self.w.log.error("Job ID 목록 형식 오류")

    def _send_0033(self):
        try:
            ps_ids = [int(x.strip()) for x in self.r_job_ps_ids.text().split(',') if x.strip()]
            self.w.send(proto.build_job_data_reply(self.r_job_id.value(), self.r_job_name.text(), ps_ids))
        except ValueError:
            self.w.log.error("PS ID 목록 형식 오류")


# ─── 탭: Tightening (MID 0060~0063) ──────────────────────────────────────────

class TighteningTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        # Integrator → Controller  [Client 전용]
        self.grp_client = QGroupBox("Integrator → Controller")
        grp_sub = self.grp_client
        g_sub = QHBoxLayout(grp_sub)
        g_sub.addWidget(_btn("MID 0060 – 타이틀링 결과 구독", lambda: self.w.send(proto.build_subscribe_tightening())))
        g_sub.addWidget(_btn("MID 0062 – 결과 ACK", lambda: self.w.send(proto.build_tightening_result_ack())))
        g_sub.addWidget(_btn("MID 0063 – 구독 해지", lambda: self.w.send(proto.build_unsubscribe_tightening())))
        layout.addWidget(grp_sub)

        # Controller → Integrator: MID 0061 타이틀링 결과  [Server 전용]
        self.grp_server = QGroupBox("MID 0061 – Last tightening result data  [Controller → Integrator]  (Rev 1 – 필드 ID 포함)")
        grp_result = self.grp_server
        g_result = QFormLayout(grp_result)

        self.cell_id = QSpinBox(); self.cell_id.setRange(0, 9999); self.cell_id.setValue(1)
        self.channel_id = QSpinBox(); self.channel_id.setRange(0, 99); self.channel_id.setValue(1)
        self.ctrl_name = QLineEdit("PF6000"); self.ctrl_name.setMaxLength(25)
        self.vin = QLineEdit(); self.vin.setMaxLength(25); self.vin.setPlaceholderText("차대번호 (VIN, 최대 25자)")
        self.job_id = QSpinBox(); self.job_id.setRange(0, 99)
        self.ps_id = QSpinBox(); self.ps_id.setRange(1, 999); self.ps_id.setValue(1)
        self.batch_size = QSpinBox(); self.batch_size.setRange(0, 9999)
        self.batch_counter = QSpinBox(); self.batch_counter.setRange(0, 9999)
        self.status = QComboBox(); self.status.addItems(["1 – OK", "0 – NOK"])
        self.torque_status = QComboBox(); self.torque_status.addItems(["1 – OK", "0 – Low", "2 – High"])
        self.angle_status = QComboBox(); self.angle_status.addItems(["1 – OK", "0 – Low", "2 – High"])
        self.batch_status = QComboBox(); self.batch_status.addItems(["1 – Batch OK", "0 – Batch NOK", "2 – Not used", "3 – Running"])
        self.torque_min = QDoubleSpinBox(); self.torque_min.setRange(0, 9999); self.torque_min.setValue(18.0); self.torque_min.setSuffix(" Nm")
        self.torque_max = QDoubleSpinBox(); self.torque_max.setRange(0, 9999); self.torque_max.setValue(22.0); self.torque_max.setSuffix(" Nm")
        self.torque_target = QDoubleSpinBox(); self.torque_target.setRange(0, 9999); self.torque_target.setValue(20.0); self.torque_target.setSuffix(" Nm")
        self.final_torque = QDoubleSpinBox(); self.final_torque.setRange(0, 9999); self.final_torque.setValue(20.1); self.final_torque.setDecimals(3); self.final_torque.setSuffix(" Nm")
        self.angle_min = QSpinBox(); self.angle_min.setRange(0, 99999); self.angle_min.setSuffix(" °")
        self.angle_max = QSpinBox(); self.angle_max.setRange(0, 99999); self.angle_max.setValue(9999); self.angle_max.setSuffix(" °")
        self.angle_target = QSpinBox(); self.angle_target.setRange(0, 99999); self.angle_target.setValue(180); self.angle_target.setSuffix(" °")
        self.final_angle = QSpinBox(); self.final_angle.setRange(0, 99999); self.final_angle.setValue(185); self.final_angle.setSuffix(" °")

        g_result.addRow("Cell ID:", self.cell_id)
        g_result.addRow("Channel ID:", self.channel_id)
        g_result.addRow("Controller Name:", self.ctrl_name)
        g_result.addRow("VIN:", self.vin)
        g_result.addRow("Job ID:", self.job_id)
        g_result.addRow("PS ID:", self.ps_id)
        g_result.addRow("Batch Size:", self.batch_size)
        g_result.addRow("Batch Counter:", self.batch_counter)
        g_result.addRow("Tightening Status:", self.status)
        g_result.addRow("Torque Status:", self.torque_status)
        g_result.addRow("Angle Status:", self.angle_status)
        g_result.addRow("Batch Status:", self.batch_status)
        g_result.addRow("Torque Min Limit:", self.torque_min)
        g_result.addRow("Torque Max Limit:", self.torque_max)
        g_result.addRow("Torque Final Target:", self.torque_target)
        g_result.addRow("Final Torque:", self.final_torque)
        g_result.addRow("Angle Min Limit:", self.angle_min)
        g_result.addRow("Angle Max Limit:", self.angle_max)
        g_result.addRow("Final Angle Target:", self.angle_target)
        g_result.addRow("Final Angle:", self.final_angle)

        # Tightening ID 자동채번
        tid_row = QHBoxLayout()
        self.tid_from = QSpinBox()
        self.tid_from.setRange(1, 2147483647)
        self.tid_from.setValue(1)
        self.tid_from.setPrefix("FROM: ")
        self.tid_current = QSpinBox()
        self.tid_current.setRange(1, 2147483647)
        self.tid_current.setValue(1)
        self.tid_current.setReadOnly(True)
        self.tid_current.setPrefix("현재: ")
        self.tid_current.setStyleSheet("background-color: #f0f0f0;")
        btn_tid_reset = _btn("리셋", self._reset_tid)
        tid_row.addWidget(self.tid_from)
        tid_row.addWidget(self.tid_current)
        tid_row.addWidget(btn_tid_reset)
        g_result.addRow("Tightening ID:", tid_row)
        self._tid = self.tid_from.value()
        self.tid_from.valueChanged.connect(self._reset_tid)

        btn_send = _btn("MID 0061 – 타이틀링 결과 전송 (Broadcast)", self._send_0061)
        btn_send.setStyleSheet("background-color: #cce5ff; font-weight: bold;")
        grp_result.layout().addRow(btn_send)
        layout.addWidget(grp_result)
        layout.addStretch()

    def _reset_tid(self):
        self._tid = self.tid_from.value()
        self.tid_current.setValue(self._tid)

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0061(self):
        self.w.send(proto.build_tightening_result(
            cell_id=self.cell_id.value(),
            channel_id=self.channel_id.value(),
            controller_name=self.ctrl_name.text(),
            vin=self.vin.text(),
            job_id=self.job_id.value(),
            ps_id=self.ps_id.value(),
            batch_size=self.batch_size.value(),
            batch_counter=self.batch_counter.value(),
            status=int(self.status.currentText()[0]),
            torque_status=int(self.torque_status.currentText()[0]),
            angle_status=int(self.angle_status.currentText()[0]),
            batch_status=int(self.batch_status.currentText()[0]),
            torque_min=self.torque_min.value(),
            torque_max=self.torque_max.value(),
            torque_target=self.torque_target.value(),
            final_torque=self.final_torque.value(),
            angle_min=self.angle_min.value(),
            angle_max=self.angle_max.value(),
            angle_target=self.angle_target.value(),
            final_angle=self.final_angle.value(),
            tightening_id=self._tid,
        ))
        self._tid += 1
        self.tid_current.setValue(self._tid)


# ─── 탭: Alarm (MID 0070~0078) ────────────────────────────────────────────────

class AlarmTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        self.grp_client = QGroupBox("Integrator → Controller")
        grp_sub = self.grp_client
        g_sub = QHBoxLayout(grp_sub)
        g_sub.addWidget(_btn("MID 0070 – Alarm 구독", lambda: self.w.send(proto.build_alarm_subscribe())))
        g_sub.addWidget(_btn("MID 0072 – Alarm ACK", lambda: self.w.send(proto.build_alarm_ack())))
        g_sub.addWidget(_btn("MID 0073 – 구독 해지", lambda: self.w.send(proto.build_alarm_unsubscribe())))
        layout.addWidget(grp_sub)

        # MID 0071 Alarm 전송 (서버 모드)  [Server 전용]
        self.grp_server = QGroupBox("MID 0071 / 0076 – Alarm  [Controller → Integrator]")
        grp_alarm = self.grp_server
        g_alarm = QFormLayout(grp_alarm)
        self.error_code = QLineEdit("E001"); self.error_code.setMaxLength(4)
        self.ctrl_ready = QComboBox(); self.ctrl_ready.addItems(["1 – OK", "0 – NOK"])
        self.tool_ready = QComboBox(); self.tool_ready.addItems(["1 – OK", "0 – NOK"])
        g_alarm.addRow("Error Code (4자):", self.error_code)
        g_alarm.addRow("Controller Ready:", self.ctrl_ready)
        g_alarm.addRow("Tool Ready:", self.tool_ready)
        grp_alarm.layout().addRow(_btn("MID 0071 – Alarm 전송 (Broadcast)", self._send_0071))
        layout.addWidget(grp_alarm)

        # MID 0076 Alarm Status – grp_server 안에 추가 (서버 모드 전용)
        g_alarm.addRow(QLabel(""))
        g_alarm.addRow(QLabel("─── MID 0076 – Alarm status ───────────────"))
        self.alarm_active = QComboBox(); self.alarm_active.addItems(["0 – No alarm", "1 – Alarm active"])
        self.status_error_code = QLineEdit("E001"); self.status_error_code.setMaxLength(4)
        g_alarm.addRow("Alarm Active:", self.alarm_active)
        g_alarm.addRow("Status Error Code:", self.status_error_code)
        grp_alarm.layout().addRow(_btn("MID 0076 – Alarm Status 전송 (Broadcast)", self._send_0076))
        layout.addWidget(self.grp_server)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0071(self):
        self.w.send(proto.build_alarm(
            error_code=self.error_code.text(),
            ctrl_ready=int(self.ctrl_ready.currentText()[0]),
            tool_ready=int(self.tool_ready.currentText()[0]),
        ))

    def _send_0076(self):
        # MID 0076 Rev 1: 01+status(1) + 02+code(4) + 03+ctrl(1) + 04+tool(1) + 05+time(19)
        dt = datetime.now()
        time_str = dt.strftime("%Y-%m-%d:%H:%M:%S")
        data = (
            f"01{int(self.alarm_active.currentText()[0]):01d}"
            f"02{self.status_error_code.text():<4.4}"
            f"03{int(self.ctrl_ready.currentText()[0]):01d}"
            f"04{int(self.tool_ready.currentText()[0]):01d}"
            f"05{time_str}"
        )
        self.w.send(proto.Message(mid=76, data=data))


# ─── 탭: Time (MID 0080~0082) ─────────────────────────────────────────────────

class TimeTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        # Client 전용: 시간 요청 / 설정
        self.grp_client = QGroupBox("Integrator → Controller  [MID 0080 / 0082]")
        g_c = QFormLayout(self.grp_client)
        h_req = QHBoxLayout()
        h_req.addWidget(_btn("MID 0080 – 시간 요청", lambda: self.w.send(proto.build_time_request())))
        h_req.addWidget(_btn("MID 0082 – 시간 설정 (현재)", lambda: self.w.send(proto.build_set_time())))
        g_c.addRow(h_req)
        self.time_str = QLineEdit(datetime.now().strftime("%Y-%m-%d:%H:%M:%S"))
        self.time_str.setPlaceholderText("YYYY-MM-DD:HH:MM:SS")
        g_c.addRow("직접 입력:", self.time_str)
        self.grp_client.layout().addRow(_btn("MID 0082 – 시간 설정 (직접)", self._send_0082))
        layout.addWidget(self.grp_client)

        # Server 전용: 시간 응답
        self.grp_server = QGroupBox("Controller → Integrator  [MID 0081]")
        g_s = QHBoxLayout(self.grp_server)
        g_s.addWidget(_btn("MID 0081 – 시간 응답 (현재시간)", lambda: self.w.send(proto.build_time_reply())))
        layout.addWidget(self.grp_server)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0082(self):
        try:
            dt = datetime.strptime(self.time_str.text(), "%Y-%m-%d:%H:%M:%S")
            self.w.send(proto.build_set_time(dt))
        except ValueError:
            self.w.log.error("시간 형식 오류 (YYYY-MM-DD:HH:MM:SS)")


# ─── 탭: Tool (MID 0040~0046) ─────────────────────────────────────────────────

class ToolTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        self.grp_client = QGroupBox("Integrator → Controller 요청")
        grp_req = self.grp_client
        g_req = QHBoxLayout(grp_req)
        g_req.addWidget(_btn("MID 0040 – Tool 데이터 요청", lambda: self.w.send(proto.build_tool_data_request())))
        g_req.addWidget(_btn("MID 0042 – Tool 비활성화", lambda: self.w.send(proto.build_disable_tool())))
        g_req.addWidget(_btn("MID 0043 – Tool 활성화", lambda: self.w.send(proto.build_enable_tool())))
        layout.addWidget(grp_req)

        self.grp_server = QGroupBox("MID 0041 – Tool data upload reply  [Controller → Integrator]")
        grp_reply = self.grp_server
        g_reply = QFormLayout(grp_reply)
        self.tool_type = QSpinBox(); self.tool_type.setRange(0, 99); self.tool_type.setValue(12)
        self.serial = QLineEdit("PF60001234"); self.serial.setMaxLength(14)
        self.spindles = QSpinBox(); self.spindles.setRange(1, 99); self.spindles.setValue(1)
        self.motor_size = QSpinBox(); self.motor_size.setRange(0, 999); self.motor_size.setValue(62)
        self.open_end = QComboBox(); self.open_end.addItems(["0 – No", "1 – Yes"])
        self.ctrl_sn = QLineEdit("000000"); self.ctrl_sn.setMaxLength(10)
        g_reply.addRow("Tool Type:", self.tool_type)
        g_reply.addRow("Serial Number:", self.serial)
        g_reply.addRow("Number of Spindles:", self.spindles)
        g_reply.addRow("Motor Size:", self.motor_size)
        g_reply.addRow("Open End Tool:", self.open_end)
        g_reply.addRow("Controller Serial:", self.ctrl_sn)
        grp_reply.layout().addRow(_btn("MID 0041 – Tool 데이터 전송 (Broadcast)", self._send_0041))
        layout.addWidget(grp_reply)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0041(self):
        self.w.send(proto.build_tool_data_reply(
            tool_type=self.tool_type.value(),
            serial=self.serial.text(),
            spindles=self.spindles.value(),
            motor_size=self.motor_size.value(),
            open_end=int(self.open_end.currentText()[0]),
            controller_sn=self.ctrl_sn.text(),
        ))


# ─── 탭: VIN (MID 0050~0054) ──────────────────────────────────────────────────

class VinTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        self.grp_client = QGroupBox("Integrator → Controller  [MID 0050 / 0051 / 0053 / 0054]")
        g = QFormLayout(self.grp_client)
        self.vin_input = QLineEdit(); self.vin_input.setMaxLength(25); self.vin_input.setPlaceholderText("최대 25자")
        g.addRow("VIN:", self.vin_input)
        h = QHBoxLayout()
        h.addWidget(_btn("MID 0050 – VIN 설정", lambda: self.w.send(proto.build_vin_download(self.vin_input.text()))))
        h.addWidget(_btn("MID 0051 – VIN 구독", lambda: self.w.send(proto.build_vin_subscribe())))
        h.addWidget(_btn("MID 0053 – VIN ACK", lambda: self.w.send(proto.build_vin_ack())))
        h.addWidget(_btn("MID 0054 – VIN 구독 해지", lambda: self.w.send(proto.build_vin_unsubscribe())))
        self.grp_client.layout().addRow(h)
        layout.addWidget(self.grp_client)

        self.grp_server = QGroupBox("MID 0052 – Vehicle ID number  [Controller → Integrator]")
        g2 = QFormLayout(self.grp_server)
        self.vin_ctrl = QLineEdit(); self.vin_ctrl.setMaxLength(25); self.vin_ctrl.setPlaceholderText("VIN")
        g2.addRow("VIN:", self.vin_ctrl)
        self.grp_server.layout().addRow(_btn("MID 0052 – VIN 전송 (Broadcast)", self._send_0052))
        layout.addWidget(self.grp_server)
        layout.addStretch()

    def set_mode(self, mode: str):
        self.grp_client.setVisible(mode == "client")
        self.grp_server.setVisible(mode == "server")

    def _send_0052(self):
        # MID 0052 Rev 1: VIN 25자
        data = f"{self.vin_ctrl.text():<25.25}"
        self.w.send(proto.Message(mid=52, data=data))


# ─── 탭: Raw MID ──────────────────────────────────────────────────────────────

class RawTab(QWidget):
    def __init__(self, window: 'MainWindow', parent=None):
        super().__init__(parent)
        self.w = window
        layout = QVBoxLayout(self)

        grp = QGroupBox("Raw MID 직접 전송")
        g = QFormLayout(grp)
        self.mid_input = QSpinBox(); self.mid_input.setRange(0, 9999); self.mid_input.setValue(5)
        self.rev_input = QSpinBox(); self.rev_input.setRange(0, 999); self.rev_input.setValue(1)
        self.data_input = QLineEdit(); self.data_input.setPlaceholderText("Data (ASCII)")
        g.addRow("MID:", self.mid_input)
        g.addRow("Revision:", self.rev_input)
        g.addRow("Data:", self.data_input)
        grp.layout().addRow(_btn("전송", self._send_raw))
        layout.addWidget(grp)

        grp2 = QGroupBox("메시지 파싱 테스트")
        g2 = QFormLayout(grp2)
        self.parse_input = QLineEdit(); self.parse_input.setPlaceholderText("ASCII 메시지 입력")
        self.parse_output = QTextEdit(); self.parse_output.setReadOnly(True); self.parse_output.setMaximumHeight(100)
        g2.addRow("Input:", self.parse_input)
        g2.addRow("Result:", self.parse_output)
        grp2.layout().addRow(_btn("파싱", self._do_parse))
        layout.addWidget(grp2)
        layout.addStretch()

    def _send_raw(self):
        self.w.send(proto.Message(mid=self.mid_input.value(), data=self.data_input.text(), revision=self.rev_input.value()))

    def _do_parse(self):
        raw = self.parse_input.text()
        msg = proto.Message.from_bytes(raw.encode('latin-1') + b'\x00')
        if msg:
            self.parse_output.setPlainText(f"MID: {msg.mid:04d} ({msg.name})\nRevision: {msg.revision}\nData: {msg.data}")
        else:
            self.parse_output.setPlainText("파싱 실패")


# ─── 메인 윈도우 ──────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Atlas Copco PF6000 테스터  [OpenProtocol Spec R2.14.0]")
        screen = QApplication.primaryScreen().availableGeometry()
        self.resize(int(screen.width() * 0.8), int(screen.height() * 0.85))

        self._client = TcpClient(self)
        self._server = TcpServer(self)
        self._client_list: list[ClientConnection] = []

        self._keepalive_timer = QTimer(self)
        self._keepalive_timer.setInterval(10_000)  # 10초
        self._keepalive_timer.timeout.connect(self._send_keepalive)

        self._setup_ui()
        self._connect_signals()
        self._set_mode("client")

    # ── UI 구성 ──────────────────────────────────────────────────────────────

    @staticmethod
    def _scrollable(widget: QWidget) -> QScrollArea:
        scroll = QScrollArea()
        scroll.setWidget(widget)
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        return scroll

    def _setup_ui(self):
        central = QWidget()
        self.setMaximumWidth(800)
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(6)
        root.addWidget(self._build_connection_panel())

        self.tabs = QTabWidget()
        self._comm_tab       = CommTab(self)
        self._ps_tab         = ParameterSetTab(self)
        self._job_tab        = JobTab(self)
        self._tightening_tab = TighteningTab(self)
        self._alarm_tab      = AlarmTab(self)
        self._time_tab       = TimeTab(self)
        self._tool_tab       = ToolTab(self)
        self._vin_tab        = VinTab(self)
        self._raw_tab        = RawTab(self)

        self.tabs.addTab(self._scrollable(self._comm_tab),       "통신 (0001-0003)")
        self.tabs.addTab(self._scrollable(self._ps_tab),         "Parameter Set (0010-0020)")
        self.tabs.addTab(self._scrollable(self._job_tab),        "Job (0030-0038)")
        self.tabs.addTab(self._scrollable(self._tightening_tab), "Tightening (0060-0063)")
        self.tabs.addTab(self._scrollable(self._alarm_tab),      "Alarm (0070-0076)")
        self.tabs.addTab(self._scrollable(self._time_tab),       "Time (0080-0082)")
        self.tabs.addTab(self._scrollable(self._tool_tab),       "Tool (0040-0043)")
        self.tabs.addTab(self._scrollable(self._vin_tab),        "VIN (0050-0054)")
        self.tabs.addTab(self._scrollable(self._raw_tab),        "Raw MID")

        self._server_panel = self._build_server_panel()

        log_grp = QGroupBox("메시지 로그")
        log_layout = QVBoxLayout(log_grp)
        self.log = LogWidget()
        btn_clear = _btn("로그 지우기", self.log.clear)
        btn_clear.setFixedWidth(100)
        log_layout.addWidget(self.log)
        h = QHBoxLayout(); h.addStretch(); h.addWidget(btn_clear)
        log_layout.addLayout(h)

        v_splitter = QSplitter(Qt.Orientation.Vertical)
        v_splitter.addWidget(self.tabs)
        v_splitter.addWidget(self._server_panel)
        v_splitter.addWidget(log_grp)
        v_splitter.setSizes([420, 120, 260])
        root.addWidget(v_splitter, 1)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self._status_lbl = QLabel("연결 안됨")
        self.status_bar.addWidget(self._status_lbl)

    def _build_connection_panel(self) -> QGroupBox:
        grp = QGroupBox("연결 설정")
        layout = FlowLayout(grp, h_spacing=8, v_spacing=6)

        self._mode_client = QRadioButton("Client (Integrator → PF6000)")
        self._mode_server = QRadioButton("Server (PF6000 시뮬레이터 = Controller)")
        self._mode_client.setChecked(True)
        grp_btn = QButtonGroup(self)
        grp_btn.addButton(self._mode_client)
        grp_btn.addButton(self._mode_server)
        layout.addWidget(self._mode_client)
        layout.addWidget(self._mode_server)

        layout.addWidget(QLabel("Host:"))
        self._host_input = QLineEdit("127.0.0.1"); self._host_input.setFixedWidth(130)
        layout.addWidget(self._host_input)
        layout.addWidget(QLabel("Port:"))
        self._port_input = QSpinBox(); self._port_input.setRange(1, 65535); self._port_input.setValue(4545); self._port_input.setFixedWidth(100)
        layout.addWidget(self._port_input)

        self._connect_btn = QPushButton("연결"); self._connect_btn.setFixedWidth(110); self._connect_btn.setStyleSheet("font-weight: bold;")
        layout.addWidget(self._connect_btn)

        self._auto_ack       = QCheckBox("자동 ACK (MID 0005)");            self._auto_ack.setChecked(True)
        self._auto_comm_ack  = QCheckBox("자동 통신 시작 ACK (MID 0002)");  self._auto_comm_ack.setChecked(True)
        layout.addWidget(self._auto_ack)
        layout.addWidget(self._auto_comm_ack)

        self._mode_client.toggled.connect(lambda c: self._set_mode("client" if c else "server"))
        self._connect_btn.clicked.connect(self._on_connect_clicked)
        return grp

    def _build_server_panel(self) -> QGroupBox:
        grp = QGroupBox("연결된 클라이언트 (서버 모드)")
        layout = QVBoxLayout(grp)
        self._client_list_widget = QListWidget()
        layout.addWidget(self._client_list_widget)
        return grp

    # ── 시그널 연결 ──────────────────────────────────────────────────────────

    def _connect_signals(self):
        self._client.connected.connect(self._on_client_connected)
        self._client.disconnected.connect(self._on_client_disconnected)
        self._client.error_occurred.connect(lambda e: self.log.error(f"소켓: {e}"))
        self._client.message_received.connect(self._on_message_from_server)

        self._server.client_connected.connect(self._on_server_client_connected)
        self._server.client_disconnected.connect(self._on_server_client_disconnected)
        self._server.message_received.connect(self._on_message_from_client)
        self._server.error_occurred.connect(lambda e: self.log.error(f"서버: {e}"))

    # ── 모드 전환 ────────────────────────────────────────────────────────────

    def _set_mode(self, mode: str):
        self._mode = mode
        is_server = (mode == "server")
        self._host_input.setEnabled(not is_server)
        self._server_panel.setVisible(is_server)
        self._auto_ack.setVisible(is_server)
        self._auto_comm_ack.setVisible(is_server)
        if is_server:
            self._connect_btn.setText("리슨 중지" if self._server.is_listening else "리슨 시작")
        else:
            self._connect_btn.setText("연결 해제" if self._client.is_connected else "연결")

        # 각 탭의 모드별 섹션 표시 전환
        for tab in (self._comm_tab, self._ps_tab, self._job_tab,
                    self._tightening_tab, self._alarm_tab, self._time_tab,
                    self._tool_tab, self._vin_tab):
            tab.set_mode(mode)

    # ── 연결 버튼 ────────────────────────────────────────────────────────────

    @Slot()
    def _on_connect_clicked(self):
        port = self._port_input.value()
        if self._mode == "client":
            if self._client.is_connected:
                self._client.disconnect_from()
            else:
                self.log.info(f"연결 시도: {self._host_input.text()}:{port}")
                self._client.connect_to(self._host_input.text().strip(), port)
        else:
            if self._server.is_listening:
                self._server.stop()
                self._connect_btn.setText("리슨 시작")
                self._status_lbl.setText("리슨 중지")
                self.log.info("서버 중지")
                self._mode_client.setEnabled(True)
                self._mode_server.setEnabled(True)
            else:
                try:
                    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
                        s.bind(('', port))
                except OSError:
                    self.log.error(f"포트 {port}이(가) 이미 사용 중입니다.")
                    return
                if self._server.listen(port):
                    self._connect_btn.setText("리슨 중지")
                    self._status_lbl.setText(f"리슨 중: 0.0.0.0:{port}")
                    self.log.info(f"서버 시작: 포트 {port}")
                    self._mode_client.setEnabled(False)
                    self._mode_server.setEnabled(False)

    # ── Client 이벤트 ────────────────────────────────────────────────────────

    @Slot()
    def _on_client_connected(self):
        self._connect_btn.setText("연결 해제")
        self._status_lbl.setText(f"연결됨: {self._host_input.text()}:{self._port_input.value()}")
        self.log.info("PF6000 연결됨")
        self._keepalive_timer.start()
        self._mode_client.setEnabled(False)
        self._mode_server.setEnabled(False)

    @Slot()
    def _on_client_disconnected(self):
        self._keepalive_timer.stop()
        self._connect_btn.setText("연결")
        self._status_lbl.setText("연결 안됨")
        self.log.info("연결 해제")
        self._mode_client.setEnabled(True)
        self._mode_server.setEnabled(True)

    @Slot()
    def _send_keepalive(self):
        if self._client.is_connected:
            self._client.send(proto.build_keep_alive())
            self.log.tx(proto.build_keep_alive())

    @Slot(object)
    def _on_message_from_server(self, msg: proto.Message):
        self.log.rx(msg)
        # MID 0061 자동 ACK
        if msg.mid == 61:
            self._client.send(proto.build_tightening_result_ack())
            resp = proto.build_tightening_result_ack()
            self.log.tx(resp)
        elif msg.mid == 71:
            resp = proto.build_alarm_ack()
            self._client.send(resp)
            self.log.tx(resp)
        elif msg.mid == 9999:
            self.log.info("Keep Alive 응답 수신")
        elif msg.mid == 15:
            resp = proto.build_ps_selected_ack()
            self._client.send(resp)
            self.log.tx(resp)
        elif msg.mid == 35:
            resp = proto.build_job_info_ack()
            self._client.send(resp)
            self.log.tx(resp)
        elif msg.mid == 52:
            resp = proto.build_vin_ack()
            self._client.send(resp)
            self.log.tx(resp)

    # ── Server 이벤트 ────────────────────────────────────────────────────────

    @Slot(object)
    def _on_server_client_connected(self, conn: ClientConnection):
        self._client_list.append(conn)
        self._client_list_widget.addItem(conn.address)
        self._status_lbl.setText(f"리슨 중 | 클라이언트: {self._server.client_count}개")
        self.log.info(f"클라이언트 연결: {conn.address}")
        conn.keepalive_timeout.connect(self._on_client_keepalive_timeout)

    @Slot(object)
    def _on_client_keepalive_timeout(self, conn: ClientConnection):
        self.log.warn(f"Keepalive 타임아웃 — 연결 강제 해제: {conn.address}")
        conn.disconnect()

    @Slot(object)
    def _on_server_client_disconnected(self, conn: ClientConnection):
        if conn in self._client_list:
            self._client_list.remove(conn)
        self._client_list_widget.clear()
        for c in self._client_list:
            self._client_list_widget.addItem(c.address)
        self._status_lbl.setText(f"리슨 중 | 클라이언트: {self._server.client_count}개")
        self.log.info(f"클라이언트 해제: {conn.address}")

    @Slot(object, object)
    def _on_message_from_client(self, conn: ClientConnection, msg: proto.Message):
        self.log.rx(msg)
        self._handle_client_request(conn, msg)

    # ── 서버 모드 MID 처리 ───────────────────────────────────────────────────

    def _handle_client_request(self, conn: ClientConnection, msg: proto.Message):
        """서버 모드: Integrator(클라이언트)로부터 받은 MID 처리"""
        mid = msg.mid

        def ack():
            if self._auto_ack.isChecked():
                r = proto.build_ack(mid)
                conn.send(r)
                self.log.tx(r)

        def nak(code=98):
            r = proto.build_nak(mid, code)
            conn.send(r)
            self.log.tx(r)

        # Application Communication
        if mid == 1:   # Communication Start
            if self._auto_comm_ack.isChecked():
                r = proto.build_comm_start_ack(
                    self._comm_tab.ack_cell_id.value(),
                    self._comm_tab.ack_channel_id.value(),
                    self._comm_tab.ack_name.text())
                conn.send(r); self.log.tx(r)
        elif mid == 3:   ack()   # Communication Stop
        elif mid == 9999:
            conn.reset_keepalive_timer()
            conn.send(proto.build_keep_alive())
            self.log.tx(proto.build_keep_alive())

        # Parameter Set
        elif mid == 10:  ack()   # PS ID Upload Request → ACK (MID 0011 수동 전송)
        elif mid == 12:  ack()   # PS Data Upload Request → ACK (MID 0013 수동 전송)
        elif mid == 14:  ack()   # PS Selected Subscribe
        elif mid == 16:  pass    # PS Selected ACK (no reply)
        elif mid == 17:  ack()   # PS Selected Unsubscribe
        elif mid == 18:          # Select PS → MID 0015 PS Selected 자동 응답 + Tightening 탭 자동 세팅
            ps_id = int(msg.data[:3]) if msg.data[:3].isdigit() else 1
            self._tightening_tab.ps_id.setValue(ps_id)
            self.log.info(f"[자동세팅] Tightening PS ID ← {ps_id} (MID 0018)")
            if self._auto_ack.isChecked():
                r = proto.Message(mid=15, data=f"01{ps_id:03d}")
                conn.send(r); self.log.tx(r)
        elif mid == 19:  ack()   # Set PS Batch Size
        elif mid == 20:  ack()   # Reset Batch Counter

        # Job
        elif mid == 30:  ack()   # Job ID Upload Request
        elif mid == 32:  ack()   # Job Data Upload Request
        elif mid == 34:  ack()   # Job Info Subscribe
        elif mid == 36:  pass    # Job Info ACK
        elif mid == 37:  ack()   # Job Info Unsubscribe
        elif mid == 38:          # Select Job → Tightening 탭 자동 세팅 + ACK
            job_id = int(msg.data[:2]) if msg.data[:2].isdigit() else 0
            self._tightening_tab.job_id.setValue(job_id)
            self.log.info(f"[자동세팅] Tightening Job ID ← {job_id} (MID 0038)")
            ack()
        elif mid == 39:  ack()   # Job Restart

        # Tool
        elif mid == 40:  ack()   # Tool Data Upload Request
        elif mid == 42:  ack()   # Disable Tool
        elif mid == 43:  ack()   # Enable Tool
        elif mid == 44:  ack()   # Disconnect Tool
        elif mid == 45:  ack()   # Set Calibration
        elif mid == 46:  ack()   # Set Primary Tool

        # VIN
        elif mid == 50:          # VIN Download → Tightening 탭 자동 세팅 + ACK
            vin = msg.data[:25].strip() if len(msg.data) >= 25 else msg.data.strip()
            self._tightening_tab.vin.setText(vin)
            self.log.info(f"[자동세팅] Tightening VIN ← '{vin}' (MID 0050)")
            ack()
        elif mid == 51:  ack()   # VIN Subscribe
        elif mid == 53:  pass    # VIN ACK
        elif mid == 54:  ack()   # VIN Unsubscribe

        # Tightening
        elif mid == 60:  ack()   # Subscribe Tightening
        elif mid == 62:          # Tightening Result ACK
            self.log.info("타이틀링 결과 ACK 수신")
        elif mid == 63:  ack()   # Unsubscribe Tightening

        # Alarm
        elif mid == 70:  ack()   # Alarm Subscribe
        elif mid == 72:          # Alarm ACK
            self.log.info("Alarm ACK 수신")
        elif mid == 73:  ack()   # Alarm Unsubscribe
        elif mid == 75:  pass    # Alarm Acknowledged on Controller ACK
        elif mid == 77:  pass    # Alarm Status ACK
        elif mid == 78:  ack()   # Acknowledge Alarm Remotely

        # Time
        elif mid == 80:
            r = proto.build_time_reply()
            conn.send(r); self.log.tx(r)
        elif mid == 82:  ack()   # Set Time

        else:
            nak(98)
            self.log.warn(f"알 수 없는 MID {mid:04d} → NAK(98)")

    # ── 공통 전송 ────────────────────────────────────────────────────────────

    def send(self, msg: proto.Message):
        if self._mode == "client":
            if not self._client.is_connected:
                self.log.error("연결되지 않음"); return
            self._client.send(msg)
        else:
            if self._server.client_count == 0:
                self.log.warn("연결된 클라이언트 없음")
            self._server.broadcast(msg)
        self.log.tx(msg)