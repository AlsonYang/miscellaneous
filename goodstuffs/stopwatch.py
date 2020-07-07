import webbrowser
import time
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("number", type=int, help="enter a number")
parser.add_argument(
    "-u", "--unit", type=str, default="m", help="enter a unit s, m or h or d"
)
parser.add_argument("-m", "--msg", type=str, help="enter a message as string")
args = parser.parse_args()
print(args)


def say_hello_when_finish(func):
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        webbrowser.open("https://www.youtube.com/watch?v=YQHsXMglC9A")

    return wrapper


def convert_to_sec(n, unit):
    if unit == "s":
        return n
    elif unit == "m":
        return n * 60
    elif unit == "h":
        return n * 3600
    elif unit == "d":
        return n * 3600 * 24
    else:
        raise Exception(
            "unit only takes value s,m or h which corresponds to seconds, minutes and hours"
        )


@say_hello_when_finish
def stopwatch(n, unit, msg):
    n = convert_to_sec(n, unit)
    for i in tqdm(range(n)):
        time.sleep(1)


if __name__ == "__main__":
    stopwatch(args.number, args.unit, args.msg)
