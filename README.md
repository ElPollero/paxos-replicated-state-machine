# paxos-replicated-state-machine

Este sistema e uma solucao distribuida para a gestao de reunioes, projetada para oferecer alta disponibilidade e consistencia de dados em ambientes sujeitos a falhas de rede ou de processos. A aplicacao utiliza uma implementacao robusta do algoritmo de consenso Paxos para replicar o estado entre multiplos servidores.

## Funcionalidades Tecnicas

O projeto consiste num sistema distribuido avancado com as seguintes caracteristicas:

* **Multi-Paxos com Lider Estavel**: Otimizacao do protocolo que permite a um lider eleito processar sequencias de pedidos sem a necessidade repetida da Fase 1, reduzindo a latencia e o trafego de mensagens.
* **Reconfiguracao Dinamica (Vertical Paxos)**: O sistema suporta alteracoes na composicao do cluster (aceitadores e quoruns) em tempo de execucao, vinculando cada configuracao a um numero de ballot especifico para garantir a seguranca durante a transicao.
* **Recuperacao de Logs e Analise Recursiva**: Mecanismo de recuperacao que permite a novos lideres reconstruir o historico de decisoes a partir de quoruns de configuracoes anteriores, preenchendo lacunas com operacoes no-op para manter a integridade da maquina de estados.
* **Fast Path para Operacoes de Topicos**: Implementacao de um caminho rapido para comandos de atualizacao de topicos que ignora o consenso total quando as dependencias sao satisfeitas localmente, utilizando sincronizacao por timestamps logicos.
* **Execucao Preditiva**: Capacidade de responder antecipadamente a pedidos de topicos ao detetar comandos de registo (ADD) pendentes na fila de processamento, otimizando a latencia percebida pelo cliente.

## Arquitetura do Sistema

A solucao e composta por tres componentes principais que comunicam via gRPC:

* **Servidores**: Atuam como proposers, acceptors e learners no protocolo Paxos, mantendo uma replica local do estado da aplicacao.
* **Cliente (App)**: Interface utilizada para submeter operacoes de gestao de reunioes e participantes.
* **Consola de Controlo**: Ferramenta administrativa para monitorizacao do cluster, gestao de ballots e simulacao de cenarios de teste como falhas ou atrasos de rede.

## Tecnologias Utilizadas

* **Linguagem**: Java 22.
* **Comunicacao**: gRPC e Protocol Buffers (proto3).
* **Gestao de Dependencias**: Maven.
* **Orquestracao**: Scripts de automacao para configuracao de ambiente e deploy local.

## Instalacao e Compilacao

### Requisitos
* Java 22
* Maven 3.8.4
* Protoc 3.12

### Procedimento
Para compilar todos os modulos do projeto, execute o seguinte comando na raiz:
```bash
mvn clean install
```
### Guia de Execucao
#### Servidores
Inicie as replicas individuais especificando a porta base, o identificador e o tipo de agendamento:

```bash
cd server
mvn exec:java -Dexec.args="{porta_base} {id} {scheduler} {max_participantes}"
```
#### Cliente
Execute a aplicacao cliente para interagir com o cluster:

```bash
cd app
mvn exec:java -Dexec.args="{id} {host} {porta_base} {scheduler}"
```

#### Consola Administrativa
Utilize a interface de controlo para gerir a configuracao e ballots:

```bash
cd console
mvn exec:java -Dexec.args="{host} {porta_base} {scheduler}"
```

#### Modos de Debug
O sistema inclui mecanismos para validar a tolerancia a falhas:

Crash: Interrupcao abrupta do processo.

Freeze: Bloqueio do processamento de mensagens.

Slow-mode: Introducao de latencia artificial na rede.

