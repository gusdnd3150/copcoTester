"""
Atlas Copco Open Protocol TCP 클라이언트 / 서버
"""

from PySide6.QtCore import QObject, Signal, QByteArray, QTimer
from PySide6.QtNetwork import QTcpServer, QTcpSocket, QHostAddress

from protocol import Message, HEADER_SIZE


class TcpClient(QObject):
    """TCP 클라이언트 (PF6000에 연결)"""

    message_received = Signal(object)   # Message
    connected = Signal()
    disconnected = Signal()
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._socket = QTcpSocket(self)
        self._buffer = b""

        self._socket.connected.connect(self.connected)
        self._socket.disconnected.connect(self._on_disconnected)
        self._socket.readyRead.connect(self._on_data)
        self._socket.errorOccurred.connect(
            lambda e: self.error_occurred.emit(self._socket.errorString())
        )

    def connect_to(self, host: str, port: int):
        self._buffer = b""
        self._socket.connectToHost(host, port)

    def disconnect_from(self):
        self._socket.disconnectFromHost()

    def send(self, msg: Message):
        self._socket.write(QByteArray(msg.to_bytes()))

    def _on_disconnected(self):
        self._buffer = b""
        self.disconnected.emit()

    def _on_data(self):
        self._buffer += bytes(self._socket.readAll())
        self._process_buffer()

    def _process_buffer(self):
        while len(self._buffer) >= HEADER_SIZE:
            try:
                msg_len = int(self._buffer[:4].decode('latin-1'))
            except (ValueError, UnicodeDecodeError):
                self._buffer = self._buffer[1:]
                continue

            total = msg_len + 1  # +1 for NUL
            if len(self._buffer) < total:
                break

            raw = self._buffer[:total]
            self._buffer = self._buffer[total:]
            msg = Message.from_bytes(raw)
            if msg:
                self.message_received.emit(msg)

    @property
    def is_connected(self) -> bool:
        return self._socket.state() == QTcpSocket.SocketState.ConnectedState


class ClientConnection(QObject):
    """서버에 연결된 클라이언트 소켓 래퍼"""

    message_received = Signal(object, object)   # (ClientConnection, Message)
    disconnected = Signal(object)               # ClientConnection
    keepalive_timeout = Signal(object)          # ClientConnection

    KEEPALIVE_TIMEOUT_MS = 10_000

    def __init__(self, socket: QTcpSocket, parent=None):
        super().__init__(parent)
        self._socket = socket
        self._buffer = b""

        self._ka_timer = QTimer(self)
        self._ka_timer.setSingleShot(True)
        self._ka_timer.setInterval(self.KEEPALIVE_TIMEOUT_MS)
        self._ka_timer.timeout.connect(lambda: self.keepalive_timeout.emit(self))
        self._ka_timer.start()

        self._socket.readyRead.connect(self._on_data)
        self._socket.disconnected.connect(self._on_disconnected)

    def _on_disconnected(self):
        self._ka_timer.stop()
        self.disconnected.emit(self)

    def reset_keepalive_timer(self):
        self._ka_timer.start()

    def send(self, msg: Message):
        self._socket.write(QByteArray(msg.to_bytes()))

    def disconnect(self):
        self._socket.disconnectFromHost()

    @property
    def address(self) -> str:
        return f"{self._socket.peerAddress().toString()}:{self._socket.peerPort()}"

    def _on_data(self):
        self._buffer += bytes(self._socket.readAll())
        self._process_buffer()

    def _process_buffer(self):
        while len(self._buffer) >= HEADER_SIZE:
            try:
                msg_len = int(self._buffer[:4].decode('latin-1'))
            except (ValueError, UnicodeDecodeError):
                self._buffer = self._buffer[1:]
                continue

            total = msg_len + 1
            if len(self._buffer) < total:
                break

            raw = self._buffer[:total]
            self._buffer = self._buffer[total:]
            msg = Message.from_bytes(raw)
            if msg:
                self.message_received.emit(self, msg)


class TcpServer(QObject):
    """TCP 서버 (PF6000 시뮬레이터)"""

    client_connected = Signal(object)       # ClientConnection
    client_disconnected = Signal(object)    # ClientConnection
    message_received = Signal(object, object)  # (ClientConnection, Message)
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._server = QTcpServer(self)
        self._clients: list[ClientConnection] = []
        self._server.newConnection.connect(self._on_new_connection)

    def listen(self, port: int, host: str = "0.0.0.0") -> bool:
        if host == "0.0.0.0":
            addr = QHostAddress(QHostAddress.SpecialAddress.AnyIPv4)
        else:
            addr = QHostAddress(host)

        if not self._server.listen(addr, port):
            self.error_occurred.emit(self._server.errorString())
            return False
        return True

    def stop(self):
        self._server.close()
        for c in self._clients[:]:
            c.disconnect()

    def broadcast(self, msg: Message):
        for c in self._clients:
            c.send(msg)

    def _on_new_connection(self):
        while self._server.hasPendingConnections():
            socket = self._server.nextPendingConnection()
            conn = ClientConnection(socket, self)
            conn.message_received.connect(self.message_received)
            conn.disconnected.connect(self._on_client_disconnected)
            self._clients.append(conn)
            self.client_connected.emit(conn)

    def _on_client_disconnected(self, conn: ClientConnection):
        if conn in self._clients:
            self._clients.remove(conn)
        self.client_disconnected.emit(conn)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def is_listening(self) -> bool:
        return self._server.isListening()