#!/bin/bash

dataAtual=$(date +%Y%m%d)

if [[ ! -d "/ecommerce/vendas" ]]
then
	mkdir /ecommerce/vendas/
	mkdir /ecommerce/vendas/backup
fi
cd /ecommerce
cp ./dados_de_vendas.csv ./vendas
cp ./dados_de_vendas.csv ./vendas/backup/dados-$dataAtual.csv
cd vendas/backup
mv dados-$dataAtual.csv backup-dados-$dataAtual.csv
echo "$(date +%Y/%m/%d\ %H:%M)" >> relatorio-$dataAtual.txt
awk -F ',' 'NR==2 {split($5, date, "/"); print "Primeiro registro de venda: " date[3] "/" date[2] "/" date[1]}' backup-dados-$dataAtual.csv >> relatorio-$dataAtual.txt
tail -n 1 backup-dados-$dataAtual.csv | awk -F ',' '{split($5, date, "/"); print "Ultimo registro de venda: " date[3] "/" date[2] "/" date[1]}' >> relatorio-$dataAtual.txt
awk -F ',' '{print $2}' backup-dados-$dataAtual.csv | sort -u | awk 'END {print "'"Quantidade de itens unicos vendidos:"'", NR}' >> relatorio-$dataAtual.txt
head -n 10 backup-dados-$dataAtual.csv
head -n 10 backup-dados-$dataAtual.csv >> relatorio-$dataAtual.txt
zip backup-dados$dataAtual.zip backup-dados-$dataAtual.csv
rm backup-dados-$dataAtual.csv ../dados_de_vendas.csv


