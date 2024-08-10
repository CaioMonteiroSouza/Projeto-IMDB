# Perguntas:
    * Qual os 10 series de animação mais bem avaliadas
    * Qual o ano que mais sairam series de animação
    * media de episodios por serie de animação
    * media de episodios por serie de animação com notas altas
    * quantidade de series de animação lançadas por ano 
    * Porcetagem de linguagem das series de animação
    * Nota media por linguagem nas series de animação
    * Quantidade media de temporadas nas series com alta avaliação
    * Quantidade media de temporadas geral
    * Nota media por país de produção
    * Quantidade de series por país
    * pais com mais series no top 10


# Desafio

No desafio da Sprint 10 deveria fazer uma DashBoard com os dados que coletei e tratei nas sprints anteriores utilizando o QuickSight
Para isso configurei minha conta no QuickSight seguindo as orientações na Udemy
E assim comecei a desenvolver o DashBoard, a primeira etapa foi importar os dados do Athena para o QuickSight, porém para isso eu deveria importar todas as tabelas da camada Refined e fazer o relacionamento entre elas para que os dados possam se correlacionar assim como foi proposto no modelo dimensional

![datasets](/Sprint%2010/Evidencias/Datasets.png)

esses foram os datasets utilizados, o relacionamento das tabelas foi feito nos 2 datasets

![relacionamento](/Sprint%2010/Evidencias/Relacionamento.png)

com isso feito ja poderia desenvolver meu DashBoard

## DashBoard

A primeira coisa que fiz foi denifir qual seria o foco da minha analise e qual a melhor maneira de apresentar ela, portanto após diversas ideias e tentativas cheguei a conslusão final, uma analise com foco geografico para saber quais os paises que tem melhor desempenho com series de animação e saber como esta o desempenho dos demais paises.

Para isso eu deveria alterar o tipo de algumas colunas que estavam em meu dataset para que fossem coluna geograficas e o QuickSight trabalhasse melhor com elas.

![alteração de tipo](/Sprint%2010/Evidencias/Alterar%20tipo%20coluna.png)

com isso feito ja poderia criar mapas que apresentassem informações valiosas para a análise.

![KPI e Titulos](/Sprint%2010/Evidencias/KPI%20e%20titulos.png)

A primeira coisa que fiz foi adicionar um titulo e definir alguns KPI's importantes, como a média de temporadas, média de episodios, avaliação média.

![Filtros](/Sprint%2010/Evidencias/Filtros.png)

Em seguida defini filtros interativos no DashBoard para que caso seja necessario, um olhar mais aprofundado aos dados é possivel

![series e status](/Sprint%2010/Evidencias/Series%20e%20status.png)

algumas das primeiras vizualizações que fiz foi a criação de uma tabela que mostra quais as series existentes que estão sendo filtradas (estão vizualização funciona melhor com os filtros aplicados) e um grafico de barra que mostra a situação das series

![Linguagem das series](/Sprint%2010/Evidencias/Linguagem%20das%20series.png)

esta vizualização ajuda a ter uma ideia melhor de como as linguagens em que as series são lançadas é distribuida

![mapa series](/Sprint%2010/Evidencias/Mapa%20serie.png)

esta vizualização mostra os paises que mais criaram series de animação

![Anos e nota media](/Sprint%2010/Evidencias/Anos%20e%20nota%20media.png)

esta vizualização nos mostra os anos em que mais sairam series assim como a nota media pelos anos

![distribuição notas](/Sprint%2010/Evidencias/Distribuição%20das%20notas.png)

aqui temos a distruibuição das notas entre as series

### Segunda tela

![top series](/Sprint%2010/Evidencias/Top%20series.png)

Nesta tela temos as series mais bem avaliadas, a distribuição dos paises que as produziram e a constancia que esses paises produzem as series

![popularidade](/Sprint%2010/Evidencias/popularidade.png)

Aqui temos as popularidades das series e a popularidade media que cada pais teve no momento da analise.

## Conclusões

Com as analises feitas podemos verificar que o Japão e os Estados unidos dominam este mercado a bastante tempo, não só em quantidade como em qualidade, além de que a frança tem uma posição significativa. Além de que as series com melhor avaliação são relativamente novas, sendo a mais antiga (One piece), estando ativa até hoje, sem ter recebido seu final.

### Filtros em ação
#### Brasil
![Brasil](/Sprint%2010/Evidencias/Analise%20Brasil.png)

#### Japão
![Japão](/Sprint%2010/Evidencias/Analise%20Japão.png)

#### Estados Unidos
![Estados Unidos](/Sprint%2010/Evidencias/Analise%20Estados%20Unidos.png)

# PDF

[Pagina 1](/Sprint%2010/Evidencias/Tela%201.pdf)
[Pagina 2](/Sprint%2010/Evidencias/tela%202.pdf)

