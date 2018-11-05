
"""
An extended wordlist construct that can be loaded from sqlite3, triple files,
etc.
"""

import lingpy as lingpy
import sqlite3
import os
import datetime
try:
    import wget
except ImportError:
    print("Module wget could not be loaded, some features may not work properly.")

def load_sqlite(table, dbase, url=False, out=False):
    """
    Retrieve triples from an sqlite3 database.
    """
    if url:
        # check for short path to url
        if url.startswith('http://') or url.startswith('https://'):
            pass
        else:
            url = 'http://tsv.lingpy.org/triples/'+url
            print(url)
            
        # check if file already exists
        if os.path.isfile(dbase):
            os.rename(
                    dbase,
                    dbase+'-backup-'+str(datetime.datetime.now()).split('.')[0]
                    )
        wget.download(url, out=dbase)
        
    db = sqlite3.connect(dbase)
    cursor = db.cursor()

    cursor.execute('select * from '+table+';')

    data = cursor.fetchall()
    
    return lingpy.basic.ops.triple2tsv(data, output='dict')

def make_lexibase(list_of_taxa, list_of_concepts, entries, filename='template'):
    """
    Function creates a new LexiBase tsv-file from a list of concepts and a list
    of taxa specified by the user.
    """
    lot = lingpy.csv2list(list_of_taxa)
    loc = lingpy.csv2list(list_of_concepts)
    
    # determine the nature of the header by reading first line of lot and loc
    header = []
    header += [x.upper() for x in loc[0]]
    header += [x.upper() for x in lot[0]]
    header += [x.upper() for x in entries]

    # append the appendix for the individual fields
    fields = []
    for h in entries:
        if not h.endswith('ID'):
            fields += ['-']
        else:
            fields += ['0']

    # dermine index of main gloss
    cidx = [x.upper() for x in loc[0]].index('CONCEPT')
    tidx = [x.upper() for x in lot[0]].index('DOCULECT')

    # make text object
    text = ''
    text += 'ID'+'\t'+'\t'.join(header)+'\n'
    idx = 1

    for i in range(len(loc)-1):
        for j in range(len(lot)-1):
            print(idx,i,j)
            tmp = str(idx)+'\t'
            tmp += '\t'.join(loc[i+1])+'\t'
            tmp += '\t'.join(lot[j+1])+'\t'
            tmp += '\t'.join(fields)+'\n'
            idx += 1
            text += tmp
        text += '#\n'
    with open(filename+'.tsv', 'w') as f:
        f.write(text)
    
class LexiBase(lingpy.basic.wordlist.Wordlist):

    def __init__(self, infile, **keywords):
        
        if type(infile) == dict:
            lingpy.basic.wordlist.Wordlist.__init__(self, infile, **keywords)  
        elif type(infile) == str and infile.endswith('.triples'):
            D = lingpy.basic.ops.triple2tsv(infile, output='dict', **keywords)

            lingpy.basic.wordlist.Wordlist.__init__(self, D)
        elif 'dbase' in keywords:
            D = load_sqlite(infile, **keywords)
            self.dbase = keywords['dbase']
            lingpy.basic.wordlist.Wordlist.__init__(self, D)
        else:
            lingpy.basic.wordlist.Wordlist.__init__(self,infile, **keywords)

        self.blacklist = []
    
    def tokenize(self, override=True, preprocessing=False):

        if not preprocessing:
            preprocessing = lambda x: x

        self.add_entries('tokens', 'ipa', lambda x:
                lingpy.ipa2tokens(preprocessing(x)),override=override)

        self.add_entries('prostring','tokens', lambda x: lingpyd.prosodic_string(x,
            _output='CcV'), override)

        self.add_entries('tokens', 'tokens', lambda x: secondary_structures(x),
                override = override)

    def __getitem__(
            self,
            idx
            ):
        """
        Method allows quick access to the data by passing the integer key.
        """

        try:
            # return full data entry as list
            out = self._data[idx]
            return out
        except KeyError:
            try:
                # return data entry with specified key word
                out = self._data[idx[0]][self._header[self._alias[idx[1]]]]
                return out
            except IndexError:
                try:
                    out = self._meta[idx]
                    return out
                except:
                    pass
    
    def add_doculect(self, doculect, values):
        """
        Add a new column (like a new doculect or the like) to the data.

        NOTES
        -----
        For the moment, we assume that we are dealing with doculects and
        concepts, which may be changed later on...
        """

        # get an index for all the values in values
        converter = {}
        for value in values:
            converter[value] = {}

        for k in self:
            c = self[k,'concept']
            for value in values:
                converter[value][c] = self[k,value]

        # now, create the wordlist
        D = {}
        idx = 1
        D[0] = ['doculect', 'concept'] + values
        for k in self.concepts:
            D[idx] = [doculect,k] + [converter[value][k] for value in values]
            idx += 1
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
                blacklist += [k]

        self.blacklist += blacklist
        print("Expanded blacklist ({0} items), modifications will be carried out when re-creating the db.".format(len(blacklist)))

    def modify_value(self, source, target, column):
        """
        Modify all values from source to target in a given column.
        """

        idxs = [idx for idx in self if self[idx][self.header[column]] ==
                source]
        for idx in idxs:
            self[idx][self.header[column]] = target
        print("Modified {0} entries in colum {1}.".format(
            len(idxs), column))

    def update(self, table, dbase=None, ignore=False, verbose=False,
            delete=False):
        """
        Upload all data which was modified in the current session to the
        database, don't change those entries which have not been touched.
        """
        
        # handle kws
        dbase = dbase or self.dbase
        ignore = ignore or []
        
        # iterate over all entries in the wl, check if they have been modified
        # and update the db if this is the case, make also a note in the backup
        # file that an automatic parse has been done
        triples = sorted(
                lingpy.basic.ops.tsv2triple(self, False)
                )

        # connect to dbase
        db = sqlite3.connect(dbase)
        cursor = db.cursor()
        
        # get all triples from db
        cursor.execute('select * from '+table+' order by ID,COL,VAL;')
        data = cursor.fetchall()
        # make dict from data
        datad = dict([((a,b),c) for a,b,c in data])
        
        modified = 0
        tobemodified = []
        tobebackedup = []
        time = int(datetime.datetime.now().timestamp())

        for line in triples:
             
            if not (line[0],line[1]) in datad or datad[line[0],line[1]] != line[2]:
                bak = False
                if (line[0],line[1]) in datad:
                    tobemodified += [line]
                    bak = True

                if (line[0],line[1]) not in datad and line[2] != '':
                    datad[line[0],line[1]] = ''
                    tobemodified += [line]
                    bak = True
                    
                if bak:
                    tobebackedup += [[table]+list(line[:2])+[datad[line[0],line[1]],time,'lingpy']]
                
        cursor.execute('delete from '+table+' where ID|":"|COL in ('+
                ','.join(['"{0}:{1}"'.format(a,b) for a,b,c in
                    tobemodified])+');')

        if delete:
            dels = '('+','.join([str(x) for x in delete])+')'
            cursor.execute('delete from '+table+' where ID in '+dels+';' )

        for a,b,c in tobemodified:
            if isinstance(c, (tuple, list)):
                c = ' '.join([str(x) for x in c])
            if verbose:
                print("[i] Inserting value {0} for ID={1} and COL={2}...".format(c,a,b))
            cursor.execute('insert into '+table+' values(?,?,?);',
                    (a,b,c)
                    )
            modified += 1
        for line in tobebackedup:
            cursor.execute('insert into backup values(?,?,?,?,?,?);',
                    tuple(line))
        #cursor.execute('vacuum')
        db.commit()
        print("Automatically modified {0} cells in the data.".format(modified))

    def vacuum(self):
        # connect to dbase
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        cursor.execute('vacuum')
        db.commit()

    
    def remove_empty_rows(self, doculect, entries=['entry_in_source', 'ipa','tokens']):
        """
        Remove rows which do not contain any data.
        """

        def check(words):

            word = ''
            for w in words:
                if w:
                    word += ''.join([x for x in w if x not in ' -?!0'])
            
            if word:
                return True
            else:
                return False

        blacklist = []
        for k in self:

            if self[k,'doculect'] == doculect:

                tocheck = [self[k,entry] for entry in entries]
                if check(tocheck):
                    pass
                else:
                    blacklist += [k]
        print('Added {0} entries to the blacklist.'.format(len(blacklist)))
        self.blacklist += blacklist
    
    def add_data(self, wordlist, ignore_columns=[]):
        """
        Add new data (for example, one doculect) to the dbase.
        """        
        # first, we check for the headers in the wordlist
        headers = [k for k in wordlist.header if k not in ignore_columns or []]
        new_heads = [k for k in headers if k not in self.header]
        
        # we make a nasty shortcut here by assuming that concept column will
        # always be there 
        for nh in new_heads:
            print('ADDING HEADERS')
            self.add_entries(nh, 'concept', lambda x: '')

        # now we start manipulating the dictionary, first we need to check for
        # the maximum entry in our current wordlist [note that we may also
        # consider to check for old ids in our backup, in case we deleted them
        # before, but we leave thas as a TODO for the moment
        idx = max(self) + 1
        headline = sorted(self.header, key=lambda x: self.header[x])

        # now we make a hash for new ids, which iterates over the max-ids
        for k in wordlist:
            
            # assemble all data in the order of the header
            new_line = []

            for h in headline:
                if h in headers:
                    entry = wordlist[k,h]
                    if type(entry) == list:
                        entry = ' '.join([str(x) for x in entry])
                    new_line += [entry]
                else:
                    if self._class[h] == int:
                        new_line += [0]
                    else:
                        new_line += ['']

            self._data[idx] = new_line
            idx += 1

    def create(self, table, dbase=None, ignore=False):
        """
        Upload triple-data to sqlite3-db. Thereby, delete the previous table
        if it is still in the database.
        """
        if not dbase:
            dbase = self.dbase

        if not ignore: ignore=[]

        # get the triples
        triples = lingpy.basic.ops.tsv2triple(self,False)
        
        # connect to tatabase
        db = sqlite3.connect(dbase)
        cursor = db.cursor()

        # check if backup exists
        cursor.execute('select name from sqlite_master where type="table" and name="backup";')
        check = cursor.fetchall()
        if not check:
            print("Adding backup to data")
            cursor.execute('create table backup (FILE text, ID int, COL text, VAL text, DATE text, user text);') 

        try:
            cursor.execute('drop table '+table+';')
        except sqlite3.OperationalError:
            pass
        cursor.execute('create table '+table+' (ID int, COL text, VAL text);')
        cursor.execute('vacuum')

        # write a log for the blacklist
        with open(lingpy.rc('timestamp')+'-blacklist.log', 'w') as f:
            f.write('ID'+'\t'+'\t'.join(sorted(self.header, key=lambda x:
                self.header[x])))
            for k in self.blacklist:
                line = [str(k)]
                for entry in self[k]:
                    if isinstance(entry, list):
                        line += [' '.join([str(x) for x in entry])]
                    else:
                        line += [str(entry)]
                f.write('\t'.join(line)+'\n')

        for a,b,c in triples:
            if b.lower() not in ignore and a not in ignore and a not in \
                    self.blacklist:
                if type(c) == list:
                    c = ' '.join([str(x) for x in c])
                else:
                    c = str(c)
                cursor.execute('insert into '+table+' values (?, ?, ?);', (a, b, c))
        db.commit()
