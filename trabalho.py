import mincemeat
import glob
import csv

text_files = glob.glob('C:\\Users\\875203.PUCMINAS\\git\\big-data\\Trab\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(k,v):
    print 'map ' + k
    from stopwords import allStopWords
    for line in v.splitlines():
        for autor in line.split(":::")[1].split("::"):
            for termo in line.split(":::")[2].split():
                if (termo not in allStopWords):
                    newText = ""
                    for each in termo:
                        if each == ".":
                            each = "" #Or replace it with whatever you like.
                        newText += each
                    yield autor+':'+newText,1

def reducefn(k,v):
    print 'reduce ' + k
    return sum(v)
  

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("C:\\Users\\875203.PUCMINAS\\git\\big-data\\Trab\\RESULT.csv", 'w'))
for k, v in results.items():
     w.writerow([k,str(v).replace("[", "").replace("]","" ).replace("'", "").replace(" ", "")])
