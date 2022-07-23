from mapreduce.utils import tokenize


def test_tokenize():
    assert tokenize("""
        Hello World! This, is, a, test. ##
        @
        Bye!

        ##



    """) == tuple([
        'hello',
        'world',
        'this',
        'is',
        'a',
        'test',
        'bye',
    ])
