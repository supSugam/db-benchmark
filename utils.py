import random
import string


def generate_random_string(length):
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(length)
    )


def generate_keywords(key_length, num_keywords=2000, replicate=50):
    keywords = [generate_random_string(key_length) for _ in range(num_keywords)]
    keywords_replicated = keywords * replicate
    random.shuffle(keywords_replicated)

    return keywords_replicated
