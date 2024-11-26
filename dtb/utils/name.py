import random
import string


def generate_random_alphanumeric(length):
    if length <= 0:
        raise ValueError("Length integer needs to be larger than 0!")
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))
