# É possivel reutilizar containers?

Sim, quando se utiliza o comando (docker run nome_do_container) um container é criado, e após deixar de ser utilizados ainda continua existindo na maquina, com isso se pode ver os containers criados com o comando (docker ps -a), e assim escolher um e utilizar o comando(Docker start nome_do_container). Entretando se quando o container foi criado ele possuia a tag --rm, o container sera removido logo após sua execução.
Caso queria utilizar dados entre containers, ou em outro container um volume seria necessario, ja que assim os dados são persistidos.
