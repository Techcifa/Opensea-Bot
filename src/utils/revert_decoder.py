"""
Helpers for decoding common EVM and SeaDrop revert payloads.
"""

from eth_abi import decode
from eth_utils import keccak


COMMON_ERRORS = {
    "Error(string)": ("Error", ["string"]),
    "Panic(uint256)": ("Panic", ["uint256"]),
    "NotActive(uint256,uint256,uint256)": ("NotActive", ["uint256", "uint256", "uint256"]),
    "MintQuantityCannotBeZero()": ("MintQuantityCannotBeZero", []),
    "MintQuantityExceedsMaxMintedPerWallet(uint256,uint256)": (
        "MintQuantityExceedsMaxMintedPerWallet",
        ["uint256", "uint256"],
    ),
    "MintQuantityExceedsMaxSupply(uint256,uint256)": (
        "MintQuantityExceedsMaxSupply",
        ["uint256", "uint256"],
    ),
    "IncorrectPayment(uint256,uint256)": ("IncorrectPayment", ["uint256", "uint256"]),
    "FeeRecipientNotAllowed(address)": ("FeeRecipientNotAllowed", ["address"]),
    "PayerNotAllowed(address)": ("PayerNotAllowed", ["address"]),
}

SELECTORS = {
    keccak(text=signature)[:4].hex(): (name, types)
    for signature, (name, types) in COMMON_ERRORS.items()
}


def extract_revert_data(error: Exception | str) -> str:
    text = str(error)
    marker = "0x"
    idx = text.find(marker)
    if idx == -1:
        return ""

    hex_chars = []
    for ch in text[idx + 2:]:
        if ch.lower() in "0123456789abcdef":
            hex_chars.append(ch)
        else:
            break
    return "0x" + "".join(hex_chars) if hex_chars else ""


def decode_revert_data(data: str) -> str:
    if not data:
        return ""
    data = data.lower()
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 8:
        return ""

    selector = data[:8]
    error_def = SELECTORS.get(selector)
    if not error_def:
        return f"Unknown custom error 0x{selector}"

    name, types = error_def
    if not types:
        return f"{name}()"

    try:
        payload = bytes.fromhex(data[8:])
        values = decode(types, payload)
    except Exception:
        return f"{name}(decode failed)"

    args = ", ".join(f"{typ}={value}" for typ, value in zip(types, values))
    return f"{name}({args})"


def decode_revert_error(error: Exception | str) -> str:
    data = extract_revert_data(error)
    decoded = decode_revert_data(data)
    return decoded or str(error)
