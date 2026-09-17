"""Create OPERATOR_PASSPHRASE_HASH for the API without echoing the passphrase.

Usage: python scripts/hash_passphrase.py   (prompts twice; prints scrypt:n:r:p:salt:hash)
Store the output in .env (local) or Secret Manager (GCP). Never commit it next to the passphrase.
"""
import getpass
import hashlib
import os
import sys

N, R, P = 2**15, 8, 1

pw = getpass.getpass("Operator passphrase (min 12 chars): ")
if len(pw) < 12:
    sys.exit("passphrase too short")
if getpass.getpass("Repeat: ") != pw:
    sys.exit("passphrases do not match")
salt = os.urandom(16)
digest = hashlib.scrypt(pw.encode(), salt=salt, n=N, r=R, p=P, dklen=32, maxmem=256 * 1024 * 1024)
print(f"scrypt:{N}:{R}:{P}:{salt.hex()}:{digest.hex()}")
