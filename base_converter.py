"""Receive any Input and stream it to a socket."""
import json
import socket
from dataclasses import asdict
from queue import Queue
from threading import Event, Thread

from converter_output import ConverterOuput
from helpers import NpEncoder


class BaseConverter:
    """Base Class for all converters."""

    def __init__(self, ip_server: str = "127.0.0.1", port_server: int = 42042, dict_flg: bool = True) -> None:
        """Initialize converter base. Start server that streams from self.buffer.

        Args:
            ip_server (str): ip adress of output socket. Defaults to "127.0.0.1".
            port_server (int): port of output socket. Defaults to 42042.
            dict_flg (bool, optional): Set True for GtCommand, False for ecos. Defaults to True.
        """
        self.ip_server: str = ip_server
        self.port_server: int = port_server
        self.dict_flg = dict_flg
        self.alive: Event = Event()
        self.output_dict_flg = dict_flg

        self.buffer: Queue[ConverterOuput] = Queue(0)
        self.raw_buffer: Queue[str] = Queue(0)

        self._start_streaming()
        self.alive.wait()

    def read_input(self) -> None:
        """Read Input and fill self.buffer.

        Raises:
            NotImplementedError: override this function.
        """
        raise NotImplementedError

    def _start_streaming(self) -> None:
        self.server_thread = Thread(
            target=self._stream_buffer,
            args=(self.ip_server, self.port_server),
            daemon=True,
        )
        self.server_thread.start()

    def _stream_buffer(self, ip: str, port: int) -> None:
        server_socket = socket.socket()
        server_socket.bind((ip, port))
        server_socket.listen(10)
        self.alive.set()
        print(f"socket ready: {ip}, {port}")
        conn, _ = server_socket.accept()
        print(f"{ip} {port} got connection")
        while True:
            if not self.buffer.empty():
                converter_output = self.buffer.get()
                if self.dict_flg:
                    converter_output = asdict(converter_output)
                json_b = json.dumps(converter_output, cls=NpEncoder) + "\n"
                conn.send(json_b.encode())
                self.buffer.task_done()
            if not self.raw_buffer.empty():
                row = self.raw_buffer.get()
                conn.send(row.encode("utf-8"))
                self.raw_buffer.task_done()
