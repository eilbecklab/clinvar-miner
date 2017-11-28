from concurrent.futures import Future, ThreadPoolExecutor
from flask import render_template

def promise(fn):
    return lambda *args, **kwargs: ThreadPoolExecutor(1).submit(fn, *args, **kwargs)

def render_template_async(*args, **kwargs):
    for key in kwargs:
        if isinstance(kwargs[key], Future):
            kwargs[key] = kwargs[key].result()
    return render_template(*args, **kwargs)
