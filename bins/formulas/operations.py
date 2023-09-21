# import logging
# from .constants import X256


# def subIn256(x, y):
#     difference = x - y
#     if difference < 0:
#         difference += X256

#     return difference


# def mulDiv(a, b, c):
#     result = (a * b) // c

#     if not isinstance(result, int):
#         logging.getLogger(__name__).error(
#             f" -->>  mulDiv error: {a} * {b} // {c} = {result} converting to {int(result)}"
#         )
#         return int(result)
#     if result < 0:
#         raise ValueError(f"mulDiv: result is negative -->  {a} * {b} // {c} = {result}")

#     return result
