from base_converter import BaseConverter

class EcosConverter(BaseConverter):
    def __init__(self, ip_server: str = "127.0.0.1", port_server: int = 42043) -> None:
        super().__init__(ip_server, port_server, False)

    def read_input(self) -> None:
        while True:
            print("\nid:")
            ecos_id = input()
            print("state: ")
            state = input()
            state_event = f"<EVENT {ecos_id}> state[{state}] <END 0 (OK)>"
            print(state_event)
            self.buffer.put(state_event)