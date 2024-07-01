import pytest

from lexibase.util import *


@pytest.mark.parametrize(
    'in_,out',
    [
        (None, 'None'),
        (1, '1'),
        ([1, 2], '1 2'),
        (('a', 'b'), 'a b'),
    ]
)
def test_stringval(in_, out):
    assert stringval(in_) == out


def test_download(mocker, capsys):
    def retrieve(*args, **kw):
        for i in range(1000):
            kw['reporthook'](b=i, tsize=1000)

    mocker.patch('lexibase.util.urlretrieve', retrieve)
    download('/', None)
