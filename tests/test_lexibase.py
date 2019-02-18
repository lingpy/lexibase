from pathlib import Path
from shutil import copy
import itertools

import pytest

from lexibase import LexiBase
from lexibase.lexibase import cursor


@pytest.fixture
def tmpdb(tmpdir):
    return str(tmpdir.join('tmp.sqlite3'))


@pytest.fixture
def lexibase_instance(tmpdb):
    lb = LexiBase({0: ['ID', 'DOCULECT', 'CONCEPT']}, dbase=tmpdb)
    lb.create()
    return lb


@pytest.fixture
def germanic(tmpdir):
    tmpdb = str(tmpdir.join('germanic.sqlite3'))
    copy(str(Path(__file__).parent.joinpath('germanic.sqlite3')), tmpdb)
    return LexiBase.from_dbase(dbase=tmpdb)


def test_LexiBase_from_triples():
    res = LexiBase(str(Path(__file__).parent / 'test.triples'))
    assert len(res) == 1


def test_LexiBase_from_url(mocker, germanic):
    def download(url, out):
        copy(str(Path(__file__).parent.joinpath('germanic.sqlite3')), str(out))

    mocker.patch('lexibase.lexibase.download', download)
    LexiBase.from_dbase(dbase=germanic.dbase, url='abc')
    assert list(germanic.dbase.parent.glob('*-backup-*'))


def test_LexiBase_cursor(tmpdb):
    lb = LexiBase({0: ['ID', 'DOCULECT', 'CONCEPT']}, dbase=tmpdb)
    with lb.cursor() as cu:
        cu.execute('create table tmp (col text)')
        cu.execute("insert into tmp values ('abc')")

    with cursor(tmpdb) as cu:
        cu.execute('select * from tmp')
        assert len(cu.fetchall()) == 1


def test_LexiBase_create(tmpdb):
    lb = LexiBase({0: ['ID', 'DOCULECT', 'CONCEPT']}, dbase=tmpdb)
    lb.create()

    with cursor(tmpdb) as cu:
        for table in ['backup', 'tmp']:
            cu.execute('select * from {0}'.format(table))
            assert len(cu.fetchall()) == 0


def test_LexiBase_update(lexibase_instance):
    lb = lexibase_instance
    lb.update()


def test_LexiBase_add_data(tmpdb, capsys):
    class Wordlist(dict):
        def __init__(self, *entries):
            dict.__init__(self)
            self.header = sorted(set(itertools.chain(*[e.keys() for e in entries])))
            self[0] = self.header
            for i, e in enumerate(entries):
                self[i] = {h: e.get(h, '') for h in self.header}

        def __getitem__(self, item):
            if isinstance(item, tuple) and len(item) == 2:
                return self[item[0]][item[1]]
            return dict.__getitem__(self, item)

    lb = LexiBase({0: ['id', 'doculect', 'concept']}, dbase=tmpdb)
    lb.create()
    idx = lb.add_data(Wordlist(dict(id=2, doculect='Spanish', x=(1, 2), concept='hand', ipa='mano')))
    assert idx
    assert lb[idx, 'X'] == (1, 2)

    assert len(lb.modify_value((1, 2), 'xyz', 'x')) == 1
    assert lb[idx, 'X'] == 'xyz'

    assert lb.update(verbose=True) == 5
    lb.modify_value('xyz', 'abc', 'x')
    assert lb.update()
    assert (1, 'DOCULECT', 'Spanish') in lb.fetchall("select * from {0}".format(lb.dbase.stem))
    assert len(lb.remove_empty_rows('Spanish', ('y'))) == 1


def test_germanic(germanic, capsys):
    ids = [r[0] for r in germanic.fetchall("select id from germanic")]
    assert len(ids) == 13554

    germanic.remove_empty_rows('German')
    germanic.remove_values('German', 'DOCULECT')
    germanic.update()
    assert germanic.fetchall("select count(*) from germanic")[0][0] == 9826
    germanic.add_doculect('Newlang', [])


def test_roundtrip_list_valued():
    #
    # FIXME!
    #
    pass
