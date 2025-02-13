# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import inspect
import os
import random
import re

import dotenv
from requests import request


def load_dotenv(sample_name: str | None = None):
    """Load environment variables from .env files.

    This function will look for two files within the working directory:
    A general `.env` file and a sample specific .env-{sample_name} file
    which has higher priority.
    """
    # load general .env
    dotenv.load_dotenv()
    # check and load sample .env
    if not sample_name:
        caller_file = inspect.stack()[1].filename
        sample_name = os.path.splitext(os.path.split(caller_file)[1])[0]

    sample_env = f'.env-{sample_name}'
    if os.path.exists(sample_env):
        print(f"Found custom .env extension: {sample_env}")
        with open(sample_env, 'r', encoding='UTF-8') as f:
            dotenv.load_dotenv(stream=f, override=True)


def read_webcontent(source_url, target_path):
    """Read web content to a local file."""
    response = request('get', source_url)
    if 200 <= response.status_code <= 299:
        with open(target_path, 'wt', encoding='utf-8') as file:
            file.write(response.text)
    else:
        raise RuntimeError('Unable to read web content. Unexpected response from web site: '
                           f'HTTP {response.status_code} {response.text}')


class RandomNameGenerator:
    """Provides randomly generated names using a public service."""

    wordlist_path = 'wordlist.txt'
    wordlist_url = 'https://raw.githubusercontent.com/mike-hearn/useapassphrase/master/js/wordlist.js'
    if not os.path.exists(wordlist_path):
        read_webcontent(wordlist_url, wordlist_path)
    with open(wordlist_path, 'rt', encoding='utf-8') as file:
        file.readline()  # skip first line
        lines = file.readlines()
    words = [re.sub('[^\\w]', '', line) for line in lines]

    @classmethod
    def random_name(cls, num: int = 3, sep: str = '_') -> str:
        """Generate a readable random name from joined random words.

        Args:
            num (int):  number of random words to concatenate
            sep (str):  concatenation separator

        Returns:
            The generated name
        """
        words = [random.choice(cls.words) for _ in range(0, num)]
        return sep.join(words)
