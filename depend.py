#!/usr/bin/env python

# this script will output a .dot file for a dependency graph

# first, run
#  python -c 'import sys; print sys.stdin.read().replace("\\\n", "")' < .depend > depend
# in the SKS source directory after make dep

# then put depend and depend.py in a common directory, choose which module to analyze the dependencies for:
module = "client"
# and execute
#  python test.py > graph.dot
# after that
# do a
#  dot -Tpng graph.dot > graph.png
# (make sure you have graphviz installed)

deps = {}

for line in open("depend"):
    if not line.strip(): continue
    s=line.split()
    f=s[0][:-1].split(".")[0]
    t=[i.split(".")[0] for i in s[1:]]
    deps[f] = t

done=[]

print "digraph G {"
def add(f):
    if f in done: return

    for i in deps[f]:
        print f,"->",i,";"
        add(i)

    done.append(f)

add(module)       # module to draw the graph for
print "}"
