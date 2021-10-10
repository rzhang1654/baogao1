#!/usr/bin/env python
from processing import Process,Queue,Pool
import time
import subprocess
import sys
import json
import paramiko
import re
import os
import os.path as path
import shutil
import ConfigParser
import xlsxwriter
from datetime import datetime


def getglobals(mast="master.ini"):
  if path.exists(mast):
    Config = ConfigParser.ConfigParser()
    Config.read(mast)
    its = Config.options('main')
    for i in its:
      v = Config.get('main',i)
      globals()[i] = int(v) if v.isdigit() else v


def copyweb(bf,wl):
  shutil.copy(bf,wl)
  os.chmod(wl,0o666)


def getbusdates(bu,da):
  bud_dir = []
  for k in da:
    if k in bu:
      line = [k,bu[k],da[k]]
      bud_dir.append(line)
  return bud_dir
  

def getbuilddate():
  log = 'log'
  builddate = {}
  for f in os.listdir('log'):
    logpath = log+'/'+f
    if path.getsize(logpath) != 0:
      with open(logpath) as fi:
        line = fi.readline().strip().split(',')
        builddate[line[0].strip()] = line[1]
  return builddate


def rmlogdir():
  log = 'log'
  monyear=datetime.today().strftime('-%b-%Y')
  oldlog = log + monyear

  if path.isdir(oldlog):
    try:
      shutil.rmtree(oldlog)
    except OSError as e:
      print("Error: %s : %s" % (oldlog, e.strerror))

  os.rename(log,oldlog)

  if not path.exists(log):
    os.makedirs(log)


def retrievebus(buinfofile):
  budir = {}
  split_re = re.compile(r'^(\S+)[\s\t]+(.+)$')

  with open(buinfofile) as f:
    for line in f.readlines():
      split_grp = split_re.match(line.strip())
      budir[split_grp.group(1)] = split_grp.group(2)
  return budir


def retrievehosts(sourcehost,command,confpath,localpath):
  ssh_client =paramiko.SSHClient()
  ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh_client.connect(hostname=sourcehost,username='root')
  stdin, stdout, stderr = ssh_client.exec_command(command)
  lines = stdout.readlines()

  if len(lines) > 5:
    ftp_client=ssh_client.open_sftp()
    ftp_client.get(confpath,localpath)
    ftp_client.close()

  ssh_client.close()

  return localpath


def genhostlist(rawlist):
  entry_re = re.compile(r'''^(?P<sat_id>\d+)
                              \| #seperator
                              (?P<host_name>[^\|]+)
                              \| #seperator
                              (?P<os_release>[^\|]*)
                              \| #seperator
                              (?P<host_group>[^\|]*)
                              \| #seperator
                              (?P<ip_address>[^\|]*)
                              \| #seperator
                              (?P<mac_address>[^\|]*)
                              ''',re.VERBOSE)
  with open(rawlist) as f:
     return [entry_re.match(line).groupdict()['host_name'] for line in f.readlines()]



def r8list(rawlist):
  rh8_re = re.compile(r'^\d+\|([^\|]+)\|RedHat 8\.\d+\|.+')
  with open(rawlist) as f:
    return [rh8_re.match(line).group(1).strip() for line in f.readlines() if rh8_re.match(line)]


def proqueue(i,q,t):
  while True:
    if q.empty():
      sys.exit()
    ip = q.get()
    ipfile = "log/" + ip
    ret = subprocess.call("scp  -o ConnectTimeout=15 -o BatchMode=yes -o StrictHostKeyChecking=no -o LogLevel=quiet  %s %s:%s" % (localscript,ip,tmpdir),
         shell = True,
         stdout = open('/dev/null','w'),
         stderr = open('/dev/null','w'))
    ret = subprocess.call("ssh  -o ConnectTimeout=15 -o BatchMode=yes -o StrictHostKeyChecking=no -o LogLevel=quiet %s python %s%s %s" % (ip,tmpdir,localscript,t),
         shell = True,
         stdout = open(ipfile,'w'),
         stderr = open('/dev/null','w'))


def setalt(i,q):
  rcommand = 'alternatives --set python /usr/bin/python3'
  while True:
    if q.empty():
      sys.exit()
    ip = q.get()
    ret = subprocess.call("ssh  -o ConnectTimeout=15 -o BatchMode=yes -o StrictHostKeyChecking=no -o LogLevel=quiet %s %s" % (ip,rcommand),
         shell = True,
         stdout = open('/dev/null','w'),
         stderr = open('/dev/null','w'))


def addhosttab(workbook,hsbudas,dmyformat,bold):
  worksheet = workbook.add_worksheet('Hosts')
  heads = ['Host Name','Business Unit','Build Date']
  rown = len(hsbudas)-1
  coln = len(hsbudas[0])-1
  worksheet.set_column(0,coln,45)
  worksheet.autofilter(0,0,rown,coln)
  for row_num, data in enumerate(heads):
    worksheet.write(0,row_num, data,bold)
  for row_num, row_data in enumerate(hsbudas):
    for col_num, col_data in enumerate(row_data):
      if col_num == 2:
        col_data = datetime.strptime(col_data, '%b %d %Y')
        worksheet.write(row_num+1, col_num, col_data,dmyformat)
      else:
        worksheet.write(row_num+1, col_num, col_data)


def addmonthtab(workbook,chart_data,bold):
  worksheet = workbook.add_worksheet('Month')
  headings = ['Month', 'Builds']
  row_ = len(chart_data)+1
  worksheet.write_row('A1', headings, bold)

  for row_num, row_data in enumerate(chart_data):
    for col_num, col_data in enumerate(row_data):
        worksheet.write(row_num+1, col_num, col_data)

  column_chart = workbook.add_chart({'type': 'column'})

  column_chart.add_series({
    'name': 'New Builds',
    'categories': '=Month!$A$2:$A$%s' % row_,
    'values': '=Month!$B$2:$B$%s' % row_,
    'marker': {'type': 'circle'}
  })

  column_chart.set_title ({'name': 'Satellite new build by month'})
  column_chart.set_x_axis({'name': 'Month/Year'})
  column_chart.set_y_axis({'name': 'Number of New builds'})

  worksheet.insert_chart('D2', column_chart)


def addbutab(workbook,chart_data6,chart_data12,bold):
  worksheet = workbook.add_worksheet('Owner')
  worksheet.set_column(0,9,35)
  headings = ['Owner', 'Builds']
  row_ = len(chart_data6)+1
  worksheet.write_row('A1', headings, bold)

  for row_num, row_data in enumerate(chart_data6):
    for col_num, col_data in enumerate(row_data):
        worksheet.write(row_num+1, col_num, col_data)

  pie_chart = workbook.add_chart({'type': 'pie'})

  pie_chart.add_series({
    'name': 'New Builds last 6 month',
    'categories': '=Owner!$A$2:$A$%s' % row_,
    'values': '=Owner!$B$2:$B$%s' % row_,
    'marker': {'type': 'circle'}
  })

  pie_chart.set_title ({'name': 'New build by Owner in last 6 months'})

  worksheet.insert_chart('D2', pie_chart)

  row_ = len(chart_data12)
  worksheet.write_row('A20', headings, bold)

  for row_num, row_data in enumerate(chart_data12):
    for col_num, col_data in enumerate(row_data):
        worksheet.write(row_num+20, col_num, col_data)

  pie_chart1 = workbook.add_chart({'type': 'pie'})
  row1_ = row_ + 20
  pie_chart1.add_series({
    'name': 'New Builds last 12 month',
    'categories': '=Owner!$A$21:$A$%s' % row1_,
    'values': '=Owner!$B$21:$B$%s' % row1_,
    'marker': {'type': 'circle'}
  })

  pie_chart1.set_title ({'name': 'New build by Owner last 12 months'})

  worksheet.insert_chart('D21', pie_chart1)


def getmonatt(bd):
  mdict = {}
  mlist = []
  for k,v in bd.items():
    nk = ' '.join(v.strip().split()[0:3:2])
    mdict.setdefault(nk,0)
    mdict[nk]+=1

  sklist = mdict.keys()
  sklist.sort(key = lambda date: datetime.strptime(date, '%b %Y'))
  mlist = [ [i,mdict[i]] for i in sklist ]
  return mlist


def getowattr(hblist,dys):
  def ifIndays(pdays,dstr):
    dstamp = datetime.strptime(dstr, '%b %d %Y')
    rtn = False
    rtn = True if (datetime.today() - dstamp).days < pdays else rtn
    return rtn
  return [ d[0] for d in hblist if ifIndays(dys,d[2]) ]


def getbuatt(hbdlist,hlist):
  mdict = {}
  mlist = []
  for i in hbdlist:
    k = i[1].strip().lower()
    if i[0] in hlist:
      mdict.setdefault(k,0)
      mdict[k]+=1
  sklist = mdict.keys()
  valist = mdict.values()
  valist.sort(reverse=True)
  mlist = [ [i,mdict[i]] for i in sklist if mdict[i] in valist[:5] ]
  mlist = sorted(mlist,key=lambda item: item[1])
  return mlist


def runqueue(ips,th=0):
  q = Queue()
  for ip in ips:
    q.put(ip)
  for i in range(8):
    if th == 0:
      p = Process(target=setalt, args=[i,q])
    else:
      p = Process(target=proqueue, args=[i,q,th])
    p.start()
  p.join()


def genbook(excelfile,hsbudas,montharr,buattr1,buattr2):
  workbook = xlsxwriter.Workbook(excelfile)
  bold = workbook.add_format({'bold': True})
  dmyformat = workbook.add_format({'num_format': 'd mmm yyyy'})
  addhosttab(workbook,hsbudas,dmyformat,bold)
  addmonthtab(workbook,montharr,bold)
  addbutab(workbook,buattr1,buattr2,bold)
  workbook.close()
  return excelfile


getglobals()
lpath = retrievehosts(sourcehost,command,confpath,localpath)
ips = genhostlist(lpath)
r8s = r8list(lpath)
runqueue(r8s)
busis = retrievebus(buinfofile)
rmlogdir()
runqueue(ips,thresolddays)
budates = getbuilddate()
hsbudas = getbusdates(busis,budates)
montharr = getmonatt(budates)
owattr1 = getowattr(hsbudas,halfyear)
owattr2 = getowattr(hsbudas,thresolddays)
buattr1 = getbuatt(hsbudas,owattr1)
buattr2 = getbuatt(hsbudas,owattr2)
bookfile = genbook(excelfile,hsbudas,montharr,buattr1,buattr2)
copyweb(bookfile,webloc)
