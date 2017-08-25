#===========================#
#=== MapReduce Streaming ===#
#===========================#

#=== Zadani ===#

# V souboru /home/_data/bible.txt je text bible. 
# (a) Napiste mapper a reducer programy v Pythonu, ktere provedou klasicky word count (histogram cetnosti slov), 
# viz napr. http://www.glennklockwood.com/data-intensive/hadoop/streaming.html
# Upravte mapper tak, aby vsechna slova byla malymi pismeny a byla zbavena znaku: .,;:?!(). 
# Slova jsou oddelena mezerou nebo koncem radky. Vynechejte oznaceni verse.

# (b) Upravte dale mapper tak, aby se vypisoval histogram poctu n-pismennych slov (zbavenych .,;:?!())

#=== Reseni ===#

# (a)

# spusteni skriptu a vypsani par radku vysledku
cat bible.txt | ./mapper1.py | sort | ./reducer.py > delka_slov.txt
cat delka_slov.txt | sort | head -n30

# a       8177
# aaron   319
# aaronites       2
# aaron's 31
# abaddon 1
# abagtha 1
# abana   1
# abarim  4
# abase   4
# abased  4
# abasing 1
# abated  6
# abba    3
# abda    2
# abdeel  1
# abdi    3
# abdiel  1
# abdon   8
# abednego        15
# abel    16

# (b) 

# spusteni skriptu a vypsani par radku vysledku
cat bible.txt | ./mapper2.py | sort | ./reducer.py > histogram_delek.txt
cat histogram_delek.txt | sort | head -n30

# 0       2
# 1       18096
# 2       130843
# 3       221235
# 4       175168
# 5       95654
# 6       52725
# 7       39456
# 8       25256
# 9       16743
# 10      7541
# 11      3857
# 12      1703
# 13      882
# 14      354
# 15      92
# 16      16
# 17      4
# 18      2