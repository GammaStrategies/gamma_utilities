## CHECKS
from decimal import Decimal
from bins.formulas.constants import (
    MAX_INT128,
    MAX_UINT128,
    MIN_INT128,
    MAX_INT256,
    MIN_INT256,
    MAX_UINT256,
    MAX_UINT160,
    MAX_UINT8,
    MIN_INT24,
    MAX_INT24,
)


def checkInt(number):
    if not type(number) == int:
        raise TypeError(f"Input must be an integer [{number}]")


def checkUInt128(number):
    checkInt(number)
    if number < 0 or number > MAX_UINT128:
        raise ValueError(f"Input must be between 0 and 2^128 - 1   [{number}]")


def checkInt128(number):
    checkInt(number)
    if number < MIN_INT128 or number > MAX_INT128:
        raise ValueError(f"Input must be between -2^127 and 2^127 - 1   [{number}]")


def checkInt256(number):
    checkInt(number)
    if number < MIN_INT256 or number > MAX_INT256:
        raise ValueError(f"Input must be between -2^255 and 2^255 - 1   [{number}]")


def checkUInt160(number):
    checkInt(number)
    if number < 0 or number > MAX_UINT160:
        raise ValueError(f"Input must be between 0 and 2^160 - 1   [{number}]")


def checkUInt256(number):
    checkInt(number)
    if number < 0 or number > MAX_UINT256:
        raise ValueError(f"Input must be between 0 and 2^256 - 1   [{number}]")


def checkUInt8(number):
    checkInt(number)
    if number < 0 or number > MAX_UINT8:
        raise ValueError(f"Input must be between 0 and 2^8 - 1   [{number}]")


def checkInt24(number):
    checkInt(number)
    if number < MIN_INT24 or number > MAX_INT24:
        raise ValueError(f"Input must be between -2^23 and 2^23 - 1   [{number}]")


def checkfloat(input):
    if type(input) != float:
        raise TypeError(f"Input must be a float   [{input}]")


def checkString(input):
    if type(input) != str:
        raise TypeError(f"Input must be a string   [{input}]")


def checkDecimal(input):
    if type(input) != Decimal:
        raise TypeError(f"Input must be a Decimal   [{input}]")


def checkDict(input):
    if type(input) != dict:
        raise TypeError(f"Input must be a dict   [{input}]")


def checkAccount(address):
    checkString(address)


# Mimic unsafe overflows in Solidity
def toUint256(number):
    try:
        checkUInt256(number)
    except:
        number = number & MAX_UINT256
        checkUInt256(number)
    return number


def toUint128(number):
    try:
        checkUInt128(number)
    except:
        number = number & MAX_UINT128
        checkUInt128(number)
    return number


def checkInputTypes(**kwargs):
    """General checkInput function for all functions that take input parameters"""
    if "string" in kwargs:
        loopChecking(kwargs.get("string"), checkString)
    if "decimal" in kwargs:
        loopChecking(kwargs.get("decimal"), checkDecimal)
    if "accounts" in kwargs:
        loopChecking(kwargs.get("accounts"), checkAccount)
    if "int24" in kwargs:
        loopChecking(kwargs.get("int24"), checkInt24)
    if "uint256" in kwargs:
        loopChecking(kwargs.get("uint256"), checkUInt256)
    if "int256" in kwargs:
        loopChecking(kwargs.get("int256"), checkInt256)
    if "uint160" in kwargs:
        loopChecking(kwargs.get("uint160"), checkUInt160)
    if "uint128" in kwargs:
        loopChecking(kwargs.get("uint128"), checkUInt128)
    if "int128" in kwargs:
        loopChecking(kwargs.get("int128"), checkInt128)
    if "uint8" in kwargs:
        loopChecking(kwargs.get("uint8"), checkUInt8)
    if "dict" in kwargs:
        checkDict(kwargs.get("dict"))


def loopChecking(tuple, fcn):
    try:
        iter(tuple)
    except TypeError:
        # Not iterable
        fcn(tuple)
    else:
        # Iterable
        for item in tuple:
            fcn(item)
