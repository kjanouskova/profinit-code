#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    # rozdelit podle tabulatoru a vzit jen druhy sloupec, zmensit pismo, rozdelit dle mezer
    keys = line.split('\t')[1].lower().split()
    for key in keys:
        # zbavime se interpunkce
        key = key.strip(".,;:?!()")
        value = 1
        # vypise klic, hodnota oddelene tabulatorem
        print( "%s\t%d" % (key, value) )


