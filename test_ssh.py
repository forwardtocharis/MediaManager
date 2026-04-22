"""
test_ssh.py -- Standalone SSH connection debugger.
Run: python test_ssh.py
No Flask server needed.
"""

import logging
import os
import sys
import traceback

# ── Config ────────────────────────────────────────────────────────────────────
HOST     = "192.168.68.52"
PORT     = 22
USERNAME = "Stuart"
PASSWORD = "$unRiz343"
KEY_PATH = ""
PATH_MAP = {"\\\\diskstation\\chat": "/volume1/chat"}
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-8s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("test_ssh")


def step(msg):
    print(f"\n{'='*60}\n>> {msg}\n{'='*60}")


# ── 1. Import paramiko ────────────────────────────────────────────────────────
step("1. Importing paramiko")
try:
    import paramiko
    print(f"  OK - paramiko {paramiko.__version__}")
except ImportError as e:
    print(f"  FAIL: {e}")
    sys.exit(1)

# ── 2. Check known_hosts ──────────────────────────────────────────────────────
step("2. Checking known_hosts")
known_hosts = os.path.expanduser("~/.ssh/known_hosts")
print(f"  Path: {known_hosts}")
print(f"  Exists: {os.path.exists(known_hosts)}")
if os.path.exists(known_hosts):
    with open(known_hosts) as f:
        lines = f.readlines()
    matches = [l for l in lines if HOST in l]
    print(f"  Total entries: {len(lines)}")
    print(f"  Entries for {HOST}: {len(matches)}")
    for m in matches:
        print(f"    {m.rstrip()}")

# ── 3. Probe server auth methods ──────────────────────────────────────────────
step("3. Probing server for allowed auth methods")
try:
    transport = paramiko.Transport((HOST, PORT))
    transport.connect()
    try:
        transport.auth_none(USERNAME)
    except paramiko.BadAuthenticationType as e:
        print(f"  Allowed methods: {e.allowed_types}")
    except Exception as e:
        print(f"  auth_none response: {e}")
    transport.close()
except Exception as e:
    print(f"  FAIL probing transport: {e}")

# ── 4. Password auth only ─────────────────────────────────────────────────────
step("4. Password auth only (look_for_keys=False, allow_agent=False)")
print(f"  host={HOST}  port={PORT}  user={USERNAME}  password_len={len(PASSWORD)}")
try:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=HOST, port=PORT, username=USERNAME,
              password=PASSWORD, look_for_keys=False, allow_agent=False, timeout=15)
    print("  SUCCESS - password auth worked")
    _, out, _ = c.exec_command("hostname && whoami")
    print(f"  Remote: {out.read().decode().strip()}")
    c.close()
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")

# ── 5. SFTP open directly ─────────────────────────────────────────────────────
step("5. SFTP open directly (password only)")
try:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=HOST, port=PORT, username=USERNAME,
              password=PASSWORD, look_for_keys=False, allow_agent=False, timeout=15)
    print("  Connected OK")
    sftp = c.open_sftp()
    print("  SFTP opened OK")
    try:
        contents = sftp.listdir("/")[:5]
        print(f"  / contents (first 5): {contents}")
    except Exception as e:
        print(f"  listdir / failed: {e}")
    sftp.close()
    c.close()
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()

# ── 6. SSHSession from ssh_applier ────────────────────────────────────────────
step("6. Testing SSHSession from src/ssh_applier")
try:
    sys.path.insert(0, str(os.path.dirname(os.path.abspath(__file__))))
    from src.ssh_applier import SSHSession
    print("  Creating SSHSession...")
    sess = SSHSession(
        host=HOST, port=PORT, username=USERNAME,
        password=PASSWORD or None,
        key_path=KEY_PATH or None,
        nas_path_map=PATH_MAP,
    )
    print("  SSHSession created OK")
    banner = sess.test()
    print(f"  test() returned: {banner!r}")
    test_path = "\\\\diskstation\\chat\\Movies\\foo.mkv"
    translated = sess.to_posix(test_path)
    print(f"  Path map: {test_path!r} -> {translated!r}")
    sess.close()
    print("  PASS")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()

print("\nDone.")
