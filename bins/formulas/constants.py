X32 = 2**32
X96 = 2**96
X128 = 2**128
X256 = 2**256


MAX_UINT128 = X128 - 1
MAX_UINT256 = X256 - 1
MAX_INT256 = 2**255 - 1
MIN_INT256 = -(2**255)
MAX_UINT160 = 2**160 - 1
MIN_INT24 = -(2**24)
MAX_INT24 = 2**23 - 1
MIN_INT128 = -(2**128)
MAX_INT128 = 2**127 - 1
MAX_UINT8 = 2**8 - 1

# The maximum value that can be returned from getSqrtRatioAtTick.
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
# The minimum value that can be returned from getSqrtRatioAtTick.
MIN_SQRT_RATIO = 4295128739

FixedPoint128_Q128 = 0x100000000000000000000000000000000
FixedPoint96_RESOLUTION = 96
FixedPoint96_Q96 = 0x1000000000000000000000000

ONE_IN_PIPS = 1000000

### The minimum tick that may be passed to #getSqrtRatioAtTick computed from log base 1.0001 of 2**-128
MIN_TICK = -887272
### The maximum tick that may be passed to #getSqrtRatioAtTick computed from log base 1.0001 of 2**128
MAX_TICK = -MIN_TICK
