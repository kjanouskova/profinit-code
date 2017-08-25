#=======================#
#=== (1) Pocty hracu ===#
#=======================#

#=== Zadani ===#

# (a) Kolik muzu a zen z CR je v souboru s ratingy (/home/_data/players_list_foa.txt)?

# (b) Porovnejte pocty i pro jine zeme, tak aby vypis byl ve formatu: 
# pocet zeme pohlavi (serazeno podle zeme).


#=== Reseni ===#

rm players_list_foa.txt
cp /home/_data/players_list_foa.txt .

# (a)
# Muzi: 10611
grep -E "CZE M" players_list_foa.txt | wc -l 
# Zeny: 754
grep -E "CZE F" players_list_foa.txt | wc -l

# (b)

tail -n+2 players_list_foa.txt | sed -r -e "s/^.* ([A-Z][A-Za-z]{2}) ([MF]) .*$/\1 \2/" | sort | uniq -c > players_count.txt

# nebo
grep -Eoh "\ {10}[A-Z]{3} [MF]" players_list_foa.txt | sort | uniq -c | tr -s " " > players_count.txt

#      17 AFG F
#     142 AFG M
#      36 AHO F
#     119 AHO M
#      28 ALB F
#     303 ALB M
# (ostatni zeme)


#===================#
#=== (2) Teploty ===#
#===================#

#=== Zadani ===#

# V souborech /home/_data/teplota1-6.csv a  /home/_data/teplota7-12.csv 
# jsou ulozena data o mereni teplot v USA.
# Spojte oba soubory do jednoho souboru teploty.csv tak, aby obsahovali jen 1 hlavicku s popisem sloupcu (na 1. radku)

# (a) Vyberte merici stanici AQW00061705. Ktery den v cervenci je pro poledne nejvyssi teplota a kolik to je?

# (b) Zvolte toto datum a cas: 22. zari, 17 hodin a zjistete pro tento okamzik, kolik je zaznamu. 

# (c) A dale omezte vypis jen na nazev stanice a teplotu, seradte podle nazvu stanice.

#=== Reseni ===#

cp /home/_data/teplota*.csv . 
head -1 teplota1-6.csv >> teplota.csv
tail -n+2 teplota1-6.csv >> teplota.csv
tail -n+2 teplota7-12.csv >> teplota.csv

# (a)
# Najdeme stanici a cervenec, pak poledne. 
# Pak seradime podle teploty s tim, ze oddelovac je ,
# Zajima nas posledni radek a vypiseme jen den a teplotu.
grep -E "AQW00061705,7,[0-9]+,12" teplota.csv | sort -k5,5n -t "," | tail -1 | awk -F, '{print $3, $5}'
# V cervenci byl nejteplejsi den 1. s teplotou 832 F

# (b)
# Na zacatku radku je nazev stanice, coz je nejaka posloupnost pismen a cislic. Pak je mesic, den, hodina.
grep -E "^[A-Z0-9]+,9,22,17" teplota.csv | wc
# 457 zaznamu

# (c)
# Nejdrive najdeme 22.9. 17 hodin a pak vypiseme jen nazev stanice a teplotu. Pozor na windows zakonceni radku. Seradime podle nazvu stanice.
grep -E "^[A-Z0-9]+,9,22,17" teplota.csv | awk -F, -v RS='\r\n' '{print $11, $5}' | sort | less

# ABERDEEN 676
# ABILENE RGNL AP 828
# AKRON CANTON RGNL AP 680
# ALAMOSA SAN LUIS AP 682
# ALBANY AP 673


#=================#
#=== (3) Bible ===#
#=================#

#=== Zadani ===#

# V souboru /home/_data/bible.txt je text bible.

# (a) V kolika versich bible se vyskytuje slovo "men"? Uvazujte , ze slovo muze zacinat na zacatku vety 
# (tedy muze zacinat velkym pismenem) a muze byt ukonceno teckou, carkou, strednikem, dvojteckou a vykricnikem.

# (b) V kolika versich se vyskytuje slovo "men" s vykricnikem?

# (c) Vypiste histogram vyskytu ruznych forem slova "men" ve formatu: pocet forma (pozor, mohou byt vice forem slova "men" na jedne radce).

#=== Reseni ===#

cp /home/_data/bible.txt . 

# (a) 1474  
grep -E "([[:space:]]|[.,;:\!])[Mm]en([[:space:]]|[.,:;\!])" bible.txt  | wc -l

# (b) 5 men! or Men!
grep -E "[Mm]en\!" bible.txt  | wc

# (c) 
grep -Eo "([[:space:]]|[[:punct:]])[Mm]en([[:space:]]|[[:punct:]])|^[Mm]en([[:space:]]|[[:punct:]])|([[:space:]]|[[:punct:]])[Mm]en$" bible.txt | tr -d [[:blank:]] | sort | uniq -c

# jsou tu navic i uvozovky a otazniky
# 1095 men
#  287 men,
#   45 men;
#   52 men:
#    5 men!
#   17 men?
#  134 men.
#   23 men'
#   17 Men
#    2 Men,
#    1 Men'

#====================#
#=== (4) Mineraly ===#
#====================#

# V /home/_data je soubor deposit.csv. Provedte transformaci, aby vystupem bylo: id, country, commodity.
# Bez duplikaci a id bylo jedinecne. Vystup ulozte do souboru deposit-upr.csv.

csv 3 7 </home/_data/deposits/deposit.csv | tail -n +2 | sort -u | awk '{printf("%d,%s\n", NR, $0)}' >deposit3.csv

# 1,Afghanistan,Aluminum
# 2,Afghanistan,Barite
# 3,Afghanistan,"Beryllium-niobium,tin"
# 4,Afghanistan,Chromium
# 5,Afghanistan,Copper

# Protoze zaznamy ve sloupci s mineraly jsou oddelene carkou, nestaci nasledujici reseni

cut -d "," -f 1,3,7 deposit.csv > deposit_spatne.csv
diff -ay deposit_upr.csv deposit_spatne.csv