#!/usr/bin/env python

################################################################################
# @Author Wei-Ming Chen, PhD                                                   #
# @pubmed abstracts scraper                                                    #
# @version: v1.0.0                                                             #
# @usage: python abstract_scraper.py                                           #
################################################################################

from Bio import Entrez
from queue import Queue
from threading import Thread
import sys, re, time


def main():
  # Maximum number of threads to be issued
  maxThread = 60

  # Download all PubMed abstracts
  db = 'pubmed'
  query = '"0000/01/01"[PDAT] : "3000/12/31"[PDAT]'
  retmax = 1000

  # Assemble the esearch URL
  Entrez.email = "your.name.here@example.org"
  handle = Entrez.esearch(db=db, term=query, usehistory='y')
  esout = handle.read()
  handle.close()

  f = open('esearch.xml','w')
  f.write(esout)
  f.close()

  # Parse WebEnv and QueryKey
  web = re.search('<WebEnv>(\S+)<\/WebEnv>', esout).group(1)
  key = re.search('<QueryKey>(\d+)<\/QueryKey>', esout).group(1)
  count = re.search('<Count>(\d+)<\/Count>', esout).group(1)
  
  # Retrive data in batches of 1000 via multiple threads
  queue = Queue()
  for retstart in range(0,int(count),retmax):
    queue.put([db, key, web, retstart, retmax])
  
  threads = []
  for idx in range(maxThread):
    t = multiplethread(queue,idx)
    t.start()
    threads.append(t)
    time.sleep(5 + idx % 5)

  for t in threads: t.join()

  print("All done!")


# Multiple Thread Class
class multiplethread(Thread):
  def __init__(self, queue, idx):
    Thread.__init__(self)
    self.queue = queue
    self.idx = idx

  def run(self):
    while self.queue.qsize():
      try:
        # Run jobs. After job done, pretend to sleep in 1 to 5 sec 
        db, key, web, retstart, retmax = self.queue.get()
        self.efetch(db, key, web, retstart, retmax)
        time.sleep(1+int(time.time()) % 5)
      except Exception:
        pass

  def efetch(self, db, key, web, retstart, retmax):
    while True:
      try:
        # Run your job      
        handle = Entrez.efetch(db=db, query_key=key, WebEnv=web, retstart=retstart, retmax=retmax, retmode='xml')
        efout = handle.read()
        handle.close()

        f = open(str(retstart)+'.xml', 'w')
        f.write(efout)
        f.close()

        f = open('log.txt','a')
        f.write(str(retstart) + " ")
        f.close()
  
        print(str(retstart)+"."+str(self.idx)+" ", end="", flush=True)
        
        break
      except Exception:
        # Record when running job failed
        f = open('err.txt','a')
        f.write(str(retstart) + " ")
        f.close()
        pass


# ------------------
#   Main context
# ------------------
print(sys.version)
main()
