import os
from threading import Thread

from replay_converter import ReplayConverter
from ecos_converter import EcosConverter

# dab lap
folder = "./data/got_raw_files/run8/"
files = ["02_dab"]

# measure trainswitch
folder_ts = "./data/got_raw_files/run5_ts/"
files_ts = [
    ["dab_ew01_dcc019"],
    ["dab_ew05_dcc014"]
]

if not os.path.exists(os.path.join(folder, files[0])):
    raise ValueError("you're an donkey.")


def replay_gtcommand():
    """Replay raw got files to socket."""
    rp = ReplayConverter(
        files=files, folder=folder, raw_flg=True, fixed_intervall=0.05
    )
    rp.read_input()


if __name__ == "__main__":
    Thread(target=replay_gtcommand, daemon=True).start()

    ec = EcosConverter()
    ec.read_input()
