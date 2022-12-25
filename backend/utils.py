from string import ascii_letters, digits
from random import choice


def random_string(size, chars=ascii_letters + digits):
    return ''.join(choice(chars) for _ in range(size))
