import os
import base64
import sys
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from loguru import logger
from resources.dev import config

try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception("Error while fetching details for key/iv/salt")

except Exception as e:
    print(f"Error occurred. Details: {e}")
    logger.error(f"Error occurred. Details: {e}")
    sys.exit(1)

BS = 16

def pad(data: bytes) -> bytes:
    pad_len = BS - len(data) % BS
    return data + bytes([pad_len] * pad_len)

def unpad(data: bytes) -> bytes:
    pad_len = data[-1]
    return data[:-pad_len]

def get_private_key():
    salt_bytes = salt.encode('utf-8') 
    key_bytes = key.encode('utf-8')
    kdf = PBKDF2(key_bytes, salt_bytes, dkLen=64, count=1000)
    return kdf[:32]

def encrypt(raw: str) -> str:
    raw_bytes = pad(raw.encode('utf-8'))
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    encrypted = cipher.encrypt(raw_bytes)
    return base64.b64encode(encrypted).decode('utf-8')

def decrypt(enc: str) -> str:
    enc_bytes = base64.b64decode(enc)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    decrypted = cipher.decrypt(enc_bytes)
    return unpad(decrypted).decode('utf-8')