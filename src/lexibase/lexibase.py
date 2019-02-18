"""
An extended wordlist construct that can be loaded from sqlite3, triple files,
etc.
"""

import lingpy
import sqlite3
import datetime
import pathlib
import contextlib

from csvw.dsv import UnicodeWriter

from lexibase.util import download, stringval


@contextlib.contextmanager
def cursor(fname):
    conn = sqlite3.connect(str(fname))
    try:
        yield conn.cursor()
        conn.commit()
    finally:
        conn.close()


class LexiBase(lingpy.basic.wordlist.Wordlist):
    def __init__(self, infile, **keywords):
        self.blacklist = []
        self.dbase = pathlib.Path(keywords.pop('dbase', 'template.sqlite3'))

        if type(infile) == str and infile.endswith('.triples'):
            infile = lingpy.basic.ops.triple2tsv(infile, output='dict', **keywords)
        lingpy.basic.wordlist.Wordlist.__init__(self, infile, **keywords)

    @classmethod
    def from_triples(cls, infile, **keywords):
        return cls(lingpy.basic.ops.triple2tsv(infile, output='dict'), **keywords)

    def cursor(self, dbase=None):
        return cursor(dbase or self.dbase)

    def fetchall(self, sql, params=None, dbase=None):
        with self.cursor(dbase=dbase) as cu:
            cu.execute(sql, params or ())
            res = list(cu.fetchall())
        return res

    @classmethod
    def from_dbase(cls, table=None, dbase=None, url='', **keywords):
        assert table or dbase
        dbase = pathlib.Path(dbase or '{0}.sqlite3'.format(table))
        table = table or dbase.stem
        if url:
            # check for short path to url
            if not (url.startswith('http://') or url.startswith('https://')):
                url = 'http://tsv.lingpy.org/triples/{0}'.format(url)

            # check if file already exists
            if dbase.exists():
                dbase.rename(
                    dbase.parent / '{0}-backup-{1}'.format(
                        dbase.name, datetime.datetime.now().isoformat().split('.')[0]))
            download(url, dbase)

        with cursor(dbase) as cu:
            cu.execute('select * from {0}'.format(table))
            res = list(cu.fetchall())
        return cls.from_triples(res, dbase=dbase, **keywords)

    def add_doculect(self, doculect, values):
        """
        Add a new column (like a new doculect or the like) to the data.

        NOTES
        -----
        For the moment, we assume that we are dealing with doculects and
        concepts, which may be changed later on...
        """
        # get an index for all the values in values
        converter = {value: {self[k, 'concept']: self[k, value] for k in self} for value in values}

        # now, create the wordlist
        D = {0: ['doculect', 'concept'] + values}
        for idx, k in enumerate(self.concepts, start=1):
            D[idx] = [doculect, k] + [converter[value][k] for value in values]

        wl = lingpy.Wordlist(D)
        self.add_data(wl)
        print('Successfully added new doculect template for {0}'.format(doculect))
    
    def remove_values(self, value, column):
        """
        Remove all values which match the target specification in the given
        column.
        """
        blacklist = []
        for k in self:
            if self[k, column] == value:
                blacklist.append(k)

        self.blacklist.extend(blacklist)
        print("Expanded blacklist ({0} items), modifications will be carried out when updating the db.".format(len(blacklist)))

    def modify_value(self, source, target, column):
        """
        Modify all values from source to target in a given column.
        """
        idxs = [idx for idx in self if self[idx][self.header[column]] == source]
        for idx in idxs:
            self[idx][self.header[column]] = target
        print("Modified {0} entries in colum {1}.".format(len(idxs), column))
        return idxs

    def vacuum(self, dbase=None):
        with self.cursor(dbase) as cu:
            cu.execute('vacuum')

    def remove_empty_rows(self, doculect, entries=('entry_in_source', 'ipa','tokens')):
        """
        Remove rows which do not contain any data.
        """
        def check(words):
            word = ''
            for w in words:
                if w:
                    word += ''.join([x for x in w if x not in ' -?!0'])
            return bool(word)

        blacklist = []
        for k in self:
            if self[k,'doculect'] == doculect:
                if not check([self[k, entry] for entry in entries]):
                    blacklist.append(k)
        print('Added {0} entries to the blacklist.'.format(len(blacklist)))
        self.blacklist += blacklist
        return blacklist

    def add_data(self, wordlist, ignore_columns=None):
        """
        Add new data (for example, one doculect) to the dbase.
        """
        ignore_columns = ignore_columns or []
        # first, we check for the headers in the wordlist
        headers = [k for k in wordlist.header if k not in ignore_columns]

        # we make a nasty shortcut here by assuming that concept column will always be there
        for nh in [k for k in headers if k not in self.header]:
            print('ADDING HEADERS: {0}'.format(nh))
            self.add_entries(nh, 'concept', lambda x: '')

        # now we start manipulating the dictionary, first we need to check for
        # the maximum entry in our current wordlist [note that we may also
        # consider to check for old ids in our backup, in case we deleted them
        # before, but we leave thas as a TODO for the moment
        headline = sorted(self.header, key=lambda x: self.header[x])
        idx = 0

        for idx, k in enumerate(wordlist, start=max(self if self else [0]) + 1):
            # assemble all data in the order of the header
            new_line = []

            for h in headline:
                if h in headers:
                    entry = wordlist[k,h]
                    if type(entry) == list:
                        entry = ' '.join([str(x) for x in entry])
                    new_line.append(entry)
                else:
                    new_line.append(0 if self._class[h] == int else '')

            self._data[idx] = new_line

        return idx

    def create(self, table=None, dbase=None, ignore=None):
        """
        Upload triple-data to sqlite3-db. Thereby, delete the previous table
        if it is still in the database.
        """
        dbase = pathlib.Path(dbase or self.dbase)
        table = table or dbase.stem
        ignore = ignore or []

        # write a log for the blacklist
        with UnicodeWriter(dbase.parent.joinpath(lingpy.rc('timestamp')+'-blacklist.log')) as w:
            w.writerow(['ID'] + sorted(self.header, key=lambda x: self.header[x]))
            w.writerows([[str(k)] + [stringval(e) for e in self[k]] for k in self.blacklist])

        with self.cursor(dbase) as cu:
            cu.execute(
                "CREATE TABLE IF NOT EXISTS backup (file TEXT, id INT, col TEXT, val TEXT, date TEXT, user TEXT)")
            cu.execute(
                "CREATE TABLE IF NOT EXISTS {0} (id INT, col TEXT, val TEXT)".format(table))
            cu.execute("DELETE FROM {0}".format(table))

        self.update(table=table, dbase=dbase, ignore=ignore)

    def update(self, table=None, dbase=None, verbose=False, ignore=None):
        """
        - Upload all data which was modified in the current session to the database,
        - delete entries which have been added to the blacklist,
        - don't change those entries which have not been touched.
        """
        ignore = ignore or []
        dbase = pathlib.Path(dbase or self.dbase)
        table = table or dbase.stem

        with self.cursor(dbase) as cu:
            cu.execute('SELECT id, col, val FROM {0} ORDER BY id, col, val'.format(table))
            datad = {(id_, col): val for id_, col, val in cu.fetchall()}

            modified = 0
            time = int(datetime.datetime.now().timestamp())

            # iterate over all entries in the wl, check if they have been modified
            # and update the db if this is the case, make also a note in the backup
            # file that an automatic parse has been done
            for id_, col, val in sorted(lingpy.basic.ops.tsv2triple(self, False)):
                if (col.lower() in ignore) or (id_ in ignore) or (id_ in self.blacklist):
                    continue
                val = stringval(val)
                if val != '' and ((id_, col) not in datad or (datad[id_, col] != val)):
                    update = True
                    if (id_, col) not in datad:
                        update = False
                        datad[id_, col] = ''

                    modified += 1
                    cu.execute(
                        "INSERT INTO backup (file, id, col, val, date, user) VALUES (?, ?, ?, ?, ?, ?)",
                        (table, id_, col, datad[id_, col], time, 'lingpy'))
                    if verbose:
                        print("[i] Inserting value {0} for ID={1} and COL={2}...".format(val, id_, col))
                    if update:
                        cu.execute(
                            "UPDATE {0} SET val = ? WHERE id = ? AND col = ?".format(table),
                            (val, id_, col))
                    else:
                        cu.execute(
                            "INSERT INTO {0} (id, col, val) VALUES (?, ?, ?)".format(table),
                            (id_, col, val))

            for id_ in self.blacklist:
                cu.execute('DELETE FROM {0} WHERE id = ?'.format(table), (id_,))

            print("Automatically modified {0} cells in the data.".format(modified))

        self.vacuum(dbase)
        return modified
