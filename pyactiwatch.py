'''
Parse a directory of Actiwatch data files, one per student
and emit a set of relations
subject, actiwatch, analysis_inputs, statistics, marker_list, epoch
'''
import sys
import os.path
import glob
import csv

def split(line):
  for row in csv.reader([line]):
    return row

def next(lines):
  try: 
    x = lines.next()
    return split(x)
  except StopIteration:
    # pad an extra fake row, since the last section doesn't end with one
    return [""]

def parsekeyvalue(row):
  return ([row[0][:-1], row[1], row[2]])

def issectionheader(row):
  '''Section headers are of the form ------name-------'''
  return row[0][0:5] == "-----"

def isempty(row):
  return len([x for x in row if x]) == 0

def ismetadata(row):
  '''A metadata row is a (name, value, units) triple.'''
  if isempty(row): return False
  return len(row) < 4 and row[0][0:5] != "-----"

def fastforward(lines):
  """Return next non-empty row"""
  while True:
    row = next(lines)
    nonempties = [x for x in row if x]
    if isempty(nonempties):
      continue
    return row

def sectionheader(lines):
  row = fastforward(lines)
  if issectionheader(row):
    return row[0].replace("-", " ").strip()
  else:
    raise ValueError("Expected section header; received %s" % (row,))

def metadata(lines):
  row = fastforward(lines)
  emptycount = 0
  while True:
    if isempty(row):
      emptycount += 1
      if emptycount > 1:
        break
    else:
      nonempties = [x for x in row if x]
      if ismetadata(nonempties):
        yield parsekeyvalue(row)
    row = next(lines)
  return

def tableheader(lines):
  """Read a table header from the stream of lines"""
  # The first long row is the headers
  # It's followed by a row of units
  hdr = fastforward(lines)
  units = next(lines)
  if len(units) < len(hdr):
    # No units provided, or they are wrong 
    return hdr
  else:
    return ["%s %s" % pair for pair in zip(hdr,units)]

def tablerows(lines, positionmap=None):
  """Read a sequence of rows from the stream of lines. Return data according to the positions of positionmap.
Each element of positionmap is an index position or None.  Needed because not all files have the same columns."""
  # Don't fastforward here; we might have an empty table.  Caller needs to put us at the first row.
  row = next(lines)
  if not positionmap: positionmap = range(len(row))
  while not isempty(row):
    yield [row[x] if x else "" for x in positionmap if x < len(row)]
    row = next(lines) 

def columndescriptors(lines):
  """Generate a sequence of column descriptors from the stream of lines"""
  row = fastforward(lines)

  if row[0] != "Column Title":
    raise ValueError("Expected column descriptor section beginning with 'Column Title, Notes', received %s" % row)

  spacer = next(lines) # skip a line
  while True:
    row = next(lines)
    nonempties = [x for x in row if x]
    if ismetadata(nonempties):
      yield [row[0][:-1], row[1]]
    elif isempty(row):
      break
    else:
      raise ValueError("Expected column descriptor of the form '<column title>: <notes>' or empty row; received %s" % keyvalue)

class Table(list):
  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, nm):
    self._name = nm

  @property
  def headers(self):
    return self._headers

  @headers.setter
  def headers(self, hdrs):
    self._headers = hdrs

  def adddescriptor(self, keyvalue):
    if hasattr(self, "metadata"): 
      self.metadata.append([studentid] + keyvalue)
    else:
      self.metadata = [keyvalue]

  def emit(self):
    """Write all rows to a file based on class name"""
    if not hasattr(self,"writer"):
      outfile = open("%s.csv" % self.__class__.__name__, "w")
      self.writer = csv.writer(outfile)
      self.writer.writerow(self.headers)
      
    for i, row in enumerate(self):
      self.writer.writerow(row)
    
  def flush(self):
    """Emit all rows, then delete them to reduce memory footprint"""
    self.emit()
    del self[:]

class Metadata(Table):
  def __init__(self):
    self.headers = ["studentid", "key", "value", "units"]
  
  def read(self, lines, studentid):
    """lines should be an iterator of lines, positioned before a metadata section"""

    name = sectionheader(lines)

    for kv in metadata(lines):
      self.append([studentid] + kv)

class Subject(Metadata):
  pass

class Actiwatch(Metadata):
  pass

class Analysis(Metadata):
  pass

class FileHeader(Table):
  def __init__(self):
    self.headers = ["studentid", "key", "value"]

  def read(self, lines, studentid):
    for kv in metadata(lines):
      self.append([studentid] + kv)

class Statistics(Table):

  def read(self, lines, studentid):
    name = sectionheader(lines)
    self.headers = ["studentid"] + tableheader(lines)
    spacer = next(lines) # Skip a blank row
    for tup in tablerows(lines):
      self.append([studentid] + tup)

class TableWithMetadata(Table):
  def read(self, lines, studentid):
    name = sectionheader(lines)

    for kv in columndescriptors(lines):
      self.adddescriptor(kv)

    row = tableheader(lines)
    nonempties = [x for x in row if x]
    if hasattr(self, "headers"):
      # We've already set the schema.
      # Try to massage this row into the schema
      def rowval(x):
        # for a given header, find the index in this data, or return None if it's missing
        if x in nonempties:
          return nonempties.index(x) + 1 # one more for student id
        else:
          return None
      indexes = [rowval(x) for x in self.headers]
    else:
      self.headers = ["studentid"] + row
      indexes = range(len(row))

    spacer = next(lines) # Skip a blank line after header

    for tup in tablerows(lines, indexes):
      self.append([studentid] + tup)

class MarkerList(TableWithMetadata):
  pass

class EpochData(TableWithMetadata):
  pass

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print """
Usage:
  $ python %s <file pattern>
Example:
  $ python %s sleep/*.csv
  """ % (__file__, __file__)
    sys.exit()


  fh = FileHeader()
  ap = Actiwatch()
  sp = Subject()
  ai = Analysis()
  st = Statistics() 
  mk = MarkerList()
  ep = EpochData()

  pattern = sys.argv[1]
  for fname in glob.glob(pattern):
    print fname
    contents = open(fname).read()
    lines = iter(contents.split("\r"))

    basename = os.path.split(fname)[1]
    studentid = os.path.splitext(basename)[0]

    fh.read(lines, studentid)
    sp.read(lines, studentid)
    ap.read(lines, studentid)
    ai.read(lines, studentid)
    st.read(lines, studentid)
    mk.read(lines, studentid)
    ep.read(lines, studentid)

    print len(ep), " epoch records"

    fh.flush()
    sp.flush()
    ap.flush()
    ai.flush()
    st.flush()
    mk.flush()
    ep.flush()
    
