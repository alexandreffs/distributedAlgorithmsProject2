import subprocess
import tempfile
import os
import time
import ctypes
from ctypes import wintypes

WINDOWS_DIR = r"C:\Users\alexa\Documents\GitHub\distributedAlgorithmsProject2\distalg-project2-base"

WINDOW_TITLES = [
    "Replica 1",
    "Replica 2",
    "Replica 3",
    "Client",
]


def windows_to_wsl_path(path: str) -> str:
    drive = path[0].lower()
    rest = path[2:].replace("\\", "/")
    return f"/mnt/{drive}{rest}"


def create_wsl_script(title: str, command: str) -> str:
    wsl_dir = windows_to_wsl_path(WINDOWS_DIR)

    script_content = f"""#!/bin/bash
printf '\\033]0;{title}\\007'

cd "{wsl_dir}" || {{
    echo "Could not enter directory: {wsl_dir}"
    exec bash
}}

echo "Starting {title}"
echo

{command}

echo
echo "{title} stopped or failed."
echo "This terminal will stay open."
exec bash
"""

    temp_dir = tempfile.gettempdir()
    script_path = os.path.join(temp_dir, f"{title.replace(' ', '_')}.sh")

    with open(script_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script_content)

    return windows_to_wsl_path(script_path)


def open_wsl_terminal(title: str, command: str):
    wsl_script_path = create_wsl_script(title, command)

    # Opens a new Windows Terminal window with a fixed title.
    subprocess.Popen(
        [
            "wt.exe",
            "-w",
            "new",
            "new-tab",
            "--title",
            title,
            "--suppressApplicationTitle",
            "wsl.exe",
            "bash",
            wsl_script_path,
        ],
        shell=False,
    )


def arrange_windows_2x2():
    user32 = ctypes.windll.user32

    # Get usable desktop area, excluding taskbar
    class RECT(ctypes.Structure):
        _fields_ = [
            ("left", wintypes.LONG),
            ("top", wintypes.LONG),
            ("right", wintypes.LONG),
            ("bottom", wintypes.LONG),
        ]

    SPI_GETWORKAREA = 0x0030
    work_area = RECT()
    user32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(work_area), 0)

    screen_x = work_area.left
    screen_y = work_area.top
    screen_width = work_area.right - work_area.left
    screen_height = work_area.bottom - work_area.top

    half_width = screen_width // 2
    half_height = screen_height // 2

    # Small gaps so borders are not hidden/cut
    gap = 6

    positions = {
        "Replica 1": (
            screen_x,
            screen_y,
            half_width - gap,
            half_height - gap,
        ),
        "Replica 2": (
            screen_x + half_width,
            screen_y,
            half_width - gap,
            half_height - gap,
        ),
        "Replica 3": (
            screen_x,
            screen_y + half_height,
            half_width - gap,
            half_height - gap,
        ),
        "Client": (
            screen_x + half_width,
            screen_y + half_height,
            half_width - gap,
            half_height - gap,
        ),
    }

    EnumWindows = user32.EnumWindows
    EnumWindowsProc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    GetWindowText = user32.GetWindowTextW
    GetWindowTextLength = user32.GetWindowTextLengthW
    IsWindowVisible = user32.IsWindowVisible
    MoveWindow = user32.MoveWindow
    ShowWindow = user32.ShowWindow

    SW_RESTORE = 9

    found = {}

    def callback(hwnd, lparam):
        if not IsWindowVisible(hwnd):
            return True

        length = GetWindowTextLength(hwnd)
        if length == 0:
            return True

        buffer = ctypes.create_unicode_buffer(length + 1)
        GetWindowText(hwnd, buffer, length + 1)
        window_title = buffer.value

        for title in WINDOW_TITLES:
            if title in window_title and title not in found:
                found[title] = hwnd

        return True

    for _ in range(30):
        found.clear()
        EnumWindows(EnumWindowsProc(callback), 0)

        if all(title in found for title in WINDOW_TITLES):
            break

        time.sleep(0.5)

    for title, hwnd in found.items():
        x, y, w, h = positions[title]

        # Restore first in case Windows Terminal opened maximized/snapped
        ShowWindow(hwnd, SW_RESTORE)
        time.sleep(0.1)

        MoveWindow(hwnd, x, y, w, h, True)


replica_1 = """
java -jar target/DistAlg.jar \\
  babel.address=127.0.0.1 \\
  initial_membership=127.0.0.1:34000,127.0.0.1:34001,127.0.0.1:34003 | tee raft_c9_Replica1.txt
"""

replica_2 = """
java -jar target/DistAlg.jar \\
  babel.address=127.0.0.1 \\
  babel.port=34001 \\
  server_port=35001 \\
  initial_membership=127.0.0.1:34000,127.0.0.1:34001,127.0.0.1:34003 | tee raft_c9_Replica2.txt
"""

replica_3 = """
java -jar target/DistAlg.jar \\
  babel.address=127.0.0.1 \\
  babel.port=34003 \\
  server_port=35003 \\
  initial_membership=127.0.0.1:34000,127.0.0.1:34001,127.0.0.1:34003 | tee raft_c9_Replica3.txt
"""

client = """
cd client || {
    echo "Could not enter client folder"
    exec bash
}

bash exec.sh 9 1000 127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35003 50 50 | tee raft_c9.txt
"""


open_wsl_terminal("Replica 1", replica_1)
time.sleep(1)

open_wsl_terminal("Replica 2", replica_2)
time.sleep(1)

open_wsl_terminal("Replica 3", replica_3)
time.sleep(2)

open_wsl_terminal("Client", client)

time.sleep(3)
arrange_windows_2x2()