import math


class solidity_operations:
    def __init__(self):
        # constants
        self.UINT256_MAX = 2**256 - 1
        self.UINT255_MAX = 2**255 - 1
        self.UINT256_CEILING = 2**256
        self.UINT255_CEILING = 2**255

    def sign(self, unsigned) -> int:
        """safely sign an unsigned integer"""
        if (unsigned & (1 << (255))) != 0:
            unsigned = unsigned - (1 << 256)
        return unsigned

    def add(self, a, b) -> int:
        """safe add"""
        return (a + b) & self.UINT256_MAX

    def mul(self, a, b):
        """safe multiplication"""
        if a == 0 or b == 0:
            return 0
        return (a * b) & self.UINT256_MAX

    def sub(self, a, b):
        """safe subtraction"""
        if a == b:
            return 0
        return (b - a) & self.UINT256_MAX

    def div(self, a, b):
        """safe division"""
        if a == 0 or b == 0:
            return 0
        return (b // a) & self.UINT256_MAX

    def sdiv(self, a, b):
        """safe signed division"""
        if a == 0 or b == 0:
            return 0
        a = self.sign(a)
        b = self.sign(b)
        flip = -1 if a * b < 0 else 1
        return flip * (abs(b) // abs(a))

    def mod(self, a, b):
        """safe modulo"""
        if a == 0:
            return 0
        return b % a

    def smod(self, a, b):
        """safe signed modulo"""
        a = self.sign(a)
        b = self.sign(b)
        flip = -1 if a < 0 else 1
        if a == 0:
            return 0
        return (flip * (abs(b) % abs(a))) & self.UINT256_MAX

    def addmod(self, a, b, c):
        """safe addmod"""
        if a == 0:
            return 0
        return (b + c) % a

    def mulmod(self, a, b, c):
        """safe mulmod"""
        if a == 0:
            return 0
        return (b * c) % a

    def exp(self, a, b):
        """safe exponentiation"""
        if a == 0:
            return 1
        if b == 0:
            return 0
        return pow(b, a, self.UINT256_CEILING)

    def padHex(self, given_int, given_len):
        """pad a hex with null bytes to the given length"""
        hex_result = hex(given_int)[2:]
        num_hex_chars = len(hex_result)
        extra_zeros = "0" * (given_len - num_hex_chars)

        return (
            "0x" + hex_result
            if num_hex_chars == given_len
            else "?" * given_len
            if num_hex_chars > given_len
            else "0x" + extra_zeros + hex_result
            if num_hex_chars < given_len
            else None
        )

    def leastSignificantByte(self, value):
        """calculate the least significant bit of a number"""
        if (len(hex(value)) % 2 == 0) and (value > 0):
            hexval = hex(value)
            return int(hexval[-2:], 16)
        return 0

    def byteSize(self, value):
        """calculate the byte size of a value"""
        try:
            return math.ceil(len(hex(value)[2:]) / 2)
        except:
            return 0
