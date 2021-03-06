from __future__ import print_function
import os

from twisted.python.filepath import FilePath

import warp


def createSkeleton(siteDir):
    from_dir = FilePath(warp.__file__).sibling('priv').child('skeleton')

    for entry_name in (l.strip() for l in from_dir.child("MANIFEST").open()):
        entry = from_dir.preauthChild(entry_name)

        destination = siteDir.preauthChild(entry_name)

        if destination.exists():
            continue

        print("  Copying %s" % entry_name)

        if not destination.parent().exists():
            destination.parent().makedirs()

        entry.copyTo(destination)

    print("Done! Run with 'twistd -n warp'")


def createNode(nodes, name, createIndex=True, nodeContent=""):

    segments = name.split('/')
    root = nodes

    for i, segment in enumerate(segments[:-1]):

        node = root.child(segment)
        if not node.exists():
            node.makedirs()
            node.child("__init__.py").open('w').write("")
            node.child("%s.py" % segment).open('w').write("""
def render_index(request):
  request.redirect("%s")
  return "Redirecting..."
""" % segments[i+1])

            print("Node '%s' created" % '/'.join(segments[:i+1]))

        root = node

    nodeName = segments[-1]

    node = root.child(nodeName)
    node.makedirs()
    node.child("__init__.py").open('w').write("")
    node.child("%s.py" % nodeName).open('w').write(nodeContent)

    if createIndex:
        node.child("index.mak").open('w').write("""
<%%inherit file="/site.mak"/>

This is the index page for node '%s'.
""" % nodeName)

    print("Node '%s' created" % name)
