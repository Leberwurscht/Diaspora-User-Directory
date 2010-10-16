#!/usr/bin/env python

# will output a dot file for dependency graph

# first run
#  python -c 'import sys; print sys.stdin.read().replace("\\\n", "")' < .depend > depend
# in SKS source directory after make dep

# then put depend and test.py in common directory, adapt this:
module = "client"
# and execute
#  python test.py > graph.dot
# after that, (make sure you have graphviz installed)
# do a
#  dot -Tpng graph.dot > graph.png

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
