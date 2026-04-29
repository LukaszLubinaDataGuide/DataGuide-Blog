# dataguide-demolibrary

Personal utility toolkit.

## Install

pip install -e .

## Usage

from dataguide_demolibrary.stringtools import concat, concat_sep

concat("hello", "world")            # "helloworld"
concat_sep("hello", "world")        # "hello_world"