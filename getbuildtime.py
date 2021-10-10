import os.path as path
import sys
import socket
from datetime import datetime

def getbtime(pdays):
  hstnam = socket.gethostname().split('.')[0].lower()
  extags = [ '/root/.exlib_hpoo_post_complete', '/root/.exlib_post_complete' ]
  for extag in extags:
    if path.isfile(extag):
      if (datetime.today() - datetime.fromtimestamp(path.getmtime(extag))).days < pdays:
        tstamp = datetime.fromtimestamp(path.getmtime(extag)).strftime("%b %d %Y")
        print("%s,%s" % (hstnam,tstamp))
        break

if __name__ == "__main__":
  try:
     getbtime(int(sys.argv[1]))
  except:
     pass

