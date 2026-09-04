# Trilha de Fundamentos — Go vs TypeScript (Projetos Comparativos)

## O que é essa trilha e por que ela existe

As outras trilhas do roteiro mestre (Go e TypeScript/Node.js) são avançadas — assumem que você já sabe a sintaxe básica e foca em arquitetura, infra e integração entre serviços. Essa trilha aqui é o oposto: **sem infraestrutura nenhuma** (sem Docker, sem Kubernetes, sem fila, sem banco externo, sem framework web). O objetivo é puro domínio de linguagem — lógica, estrutura de dados e os fundamentos que sustentam tudo o que vem depois.

A ideia central é **implementar o mesmo problema duas vezes, em Go e em TypeScript**, lado a lado. Isso força você a entender o problema de verdade (porque resolver ele duas vezes expõe qualquer entendimento raso) e, de quebra, mostra na prática onde as duas linguagens se parecem e onde elas resolvem a mesma coisa de um jeito fundamentalmente diferente (o caso mais óbvio disso é concorrência).

## Regras de como fazer

1. **Só documentação oficial.** Go: [go.dev/doc](https://go.dev/doc/), pacotes da standard library (`pkg.go.dev`). TypeScript: [MDN](https://developer.mozilla.org/) pra APIs de JS/runtime, [TypeScript Handbook](https://www.typescriptlang.org/docs/handbook/intro.html) pra tipagem, docs do Node.js pra APIs de sistema (`fs`, `crypto` etc).
2. **Sem IA.** Nada de pedir pra um assistente escrever ou revisar o código durante a implementação. O valor do exercício está em travar, ler a documentação, e destravar sozinho. Depois de pronto, se quiser, pode usar IA só pra revisão de código ou pra discutir uma abordagem alternativa — não pra construir.
3. **Sem framework, sem lib externa de terceiro.** Só standard library de cada linguagem (`net/http` se precisar de algo em Go, `fs`/`crypto`/`readline` nativos do Node em TS). Se o projeto pedir algo que a lib padrão não resolve bem (ex: fila de prioridade em Go, que não existe pronta), implemente a estrutura de dados você mesmo — é parte do exercício.
4. **Sem persistência em banco.** Onde precisar guardar dado entre execuções, use arquivo local (JSON, texto simples, binário) — nunca Postgres/Redis/etc. Essas ferramentas já são cobertas nas trilhas avançadas.
5. **Implemente via CLI.** Todos os projetos rodam no terminal (argumentos de linha de comando, `stdin`/`stdout`), sem interface web nem gráfica. Isso mantém o foco 100% em lógica, sem gastar tempo em UI.
6. **Faça a mesma versão nas duas linguagens antes de avançar pro próximo tema.** Não acumule "dívida" fazendo todos os 10 em Go primeiro e só depois todos em TS — a comparação lado a lado é mais rica quando as duas implementações do mesmo problema estão frescas na sua cabeça ao mesmo tempo.
7. **Documente a comparação, não só o código.** Ao terminar cada par (Go + TS do mesmo tema), escreva um bloco curto de "diferenças que notei" — nem que sejam 3-4 linhas. É esse comparativo que vira material de entrevista depois ("já implementei X nas duas linguagens e a diferença mais interessante foi...").
8. **Não pule pro difícil sem terminar o médio.** A ordem de dificuldade dentro da trilha é intencional — cada tema difícil usa uma estrutura de dados ou raciocínio que aparece de forma mais simples em algum tema médio anterior.

## Como intercalar com LeetCode e Exercism

Essa trilha, LeetCode e Exercism resolvem coisas diferentes e podem rodar em paralelo sem conflito:

- **Exercism**: sintaxe isolada, exercício pequeno e guiado, com feedback de mentor — bom aquecimento antes de um projeto novo dessa trilha.
- **LeetCode**: lógica de algoritmo pura, sem preocupação de projeto (sem CLI, sem arquivo, sem "produto final") — serve pra treinar padrões de algoritmo (two pointers, sliding window, DP) que depois você aplica dentro dos projetos difíceis daqui (ex: o tema 7 de labirinto usa exatamente BFS/DFS que aparece full time no LeetCode).
- **Essa trilha**: aplica lógica dentro de um "produto" pequeno e completo — arquivo de entrada, tratamento de erro, saída formatada. É o passo que falta entre "sei resolver o algoritmo isolado" e "sei construir algo funcional sozinho".

Sugestão de ritmo: 1 tema por semana (implementação em Go + implementação em TS + bloco de comparação), com Exercism/LeetCode rodando nos dias que sobrarem na mesma semana.

---

## 🟡 NÍVEL MÉDIO (5 temas)

### 1. Agenda de contatos com persistência em arquivo (check in golang)
**O que fazer:** CLI que aceita comandos (adicionar, listar, editar, remover contato) e persiste os dados em um arquivo JSON local, recarregando o estado a cada execução.
**Conceitos:** structs/interfaces, slices/arrays, leitura e escrita de arquivo, serialização/desserialização (`encoding/json` em Go, `JSON.parse`/`JSON.stringify` tipado em TS).
**Critério de pronto:** fechar o programa, abrir de novo, e os contatos anteriores continuarem lá.

### 2. Validador de CPF/CNPJ
**O que fazer:** implementa o algoritmo real de cálculo de dígito verificador (sem lib pronta de nenhuma das duas linguagens), aceita entrada formatada ou não (com ou sem pontuação) e informa se é válido, com mensagem específica de erro quando não for.
**Conceitos:** manipulação de string, aritmética modular, tratamento de erro tipado.
**Critério de pronto:** validar corretamente uma lista de CPFs/CNPJs conhecidos (válidos e inválidos de propósito) sem usar nenhuma biblioteca de validação pronta.

### 3. Conversor de Markdown pra HTML
**O que fazer:** mini parser que lê um arquivo `.md` com sintaxe básica (`# título`, `**negrito**`, `- item de lista`) e gera o HTML equivalente, sem usar lib de markdown pronta.
**Conceitos:** parsing de texto linha a linha, expressões regulares, recursão simples (pra lidar com formatação aninhada, ex: negrito dentro de item de lista).
**Critério de pronto:** um arquivo `.md` de teste com os três elementos suportados gera um HTML válido e visualmente correto ao abrir no navegador.

### 4. Sistema de reserva de assentos concorrente
**O que fazer:** representa uma matriz de assentos (ex: 10x10, tipo sala de cinema) e simula múltiplas "requisições" concorrentes tentando reservar o mesmo assento ao mesmo tempo — só uma pode ganhar, as outras devem ser rejeitadas de forma segura, sem duas reservas conflitantes acontecerem.
**Conceitos:** concorrência — em Go, goroutines + mutex (ou canais); em TS/Node, o modelo é bem diferente porque o event loop é single-threaded, então a "concorrência real" aparece melhor simulada com múltiplas operações assíncronas competindo (`Promise.all` disputando o mesmo recurso) ou usando `worker_threads` se quiser aproximar mais do paralelismo real do Go.
**Critério de pronto:** disparar 50 tentativas simultâneas de reservar o mesmo assento e só uma ser bem-sucedida, nenhuma condição de corrida deixando o assento "meio reservado" ou reservado duas vezes.
**Por que esse é o mais valioso do nível médio:** é onde Go e TS resolvem o mesmo problema de fila filosoficamente diferente — vale escrever no bloco de comparação exatamente essa diferença de modelo (threads reais com mutex vs. single-thread com fila de eventos).

### 5. Gerador e verificador de força de senha
**O que fazer:** duas funcionalidades no mesmo CLI — (a) gera uma senha aleatória segura com parâmetros configuráveis (tamanho, incluir símbolo, incluir número); (b) recebe uma senha digitada pelo usuário e calcula um score de força baseado em regras explícitas (comprimento, variedade de caracteres, presença em lista de senhas comuns).
**Conceitos:** geração de número aleatório criptograficamente seguro (`crypto/rand` em Go, `crypto.randomBytes` em Node — nunca `Math.random`/`math/rand` pra isso), regex, combinação de regras de validação.
**Critério de pronto:** a função de geração nunca repete o mesmo padrão previsível em execuções sucessivas, e o verificador dá score visivelmente mais baixo pra senhas fracas conhecidas (ex: `123456`, `senha123`) do que pra senhas fortes geradas pela própria ferramenta.

---

## 🔴 NÍVEL DIFÍCIL (5 temas)

### 6. Compactador Huffman
**O que fazer:** implementa compactação e descompactação de um arquivo de texto usando codificação de Huffman construída do zero (sem lib de compressão pronta) — conta frequência de caracteres, monta a árvore binária de Huffman, gera o código de cada caractere, e escreve o arquivo compactado bit a bit.
**Conceitos:** árvore binária, fila de prioridade (min-heap implementado por você, já que nem Go nem a lib padrão de Node têm heap pronto de fábrica pra isso), manipulação de bits.
**Critério de pronto:** compactar um arquivo de texto e descompactar de volta, e o conteúdo restaurado ser byte a byte idêntico ao original — além disso, o arquivo compactado deve ser visivelmente menor que o original pra um texto com repetição razoável de caracteres.

### 7. Resolvedor de labirinto (BFS/DFS)
**O que fazer:** lê uma matriz de um arquivo texto (representando paredes e caminho livre, com ponto de entrada e saída marcados) e encontra o caminho mais curto entre os dois pontos, imprimindo o caminho encontrado sobre a matriz original.
**Conceitos:** representação de grafo implícito numa matriz, busca em largura (BFS) pra caminho mais curto, busca em profundidade (DFS) como alternativa pra comparar comportamento.
**Critério de pronto:** implementar as duas buscas (BFS e DFS) pro mesmo labirinto e confirmar que o BFS sempre encontra o caminho mais curto, enquanto o DFS encontra *um* caminho, não necessariamente o mais curto — documentar essa diferença no bloco de comparação.

### 8. Mini banco chave-valor com persistência
**O que fazer:** implementa um key-value store simples com comandos `SET`, `GET`, `DELETE`, gravando cada operação num log append-only em disco (nunca sobrescreve o arquivo, só adiciona) e mantendo um índice em memória (mapa/objeto) apontando pra posição de cada chave no log. Ao reiniciar o programa, reconstrói o índice lendo o log inteiro do começo (simulando recuperação de estado após um "crash").
**Conceitos:** I/O de baixo nível em arquivo, hashing/mapa como índice, o conceito de log append-only que fundamenta bancos de dados reais e sistemas como Kafka.
**Critério de pronto:** fazer várias operações, "matar" o processo sem fechar graciosamente, subir de novo, e o índice reconstruído bater exatamente com o estado esperado (última operação válida por chave).

### 9. Interpretador de uma mini-linguagem de regras
**O que fazer:** cria uma linguagem de regras bem pequena e própria sua (ex: `SE idade > 18 ENTAO liberado SENAO negado`), com um tokenizador que quebra o texto em tokens, um parser que monta uma árvore de sintaxe a partir dos tokens, e um interpretador que percorre essa árvore e executa a regra contra um dado de entrada.
**Conceitos:** tokenização (léxico), parsing recursivo (sintático), árvore de sintaxe abstrata (AST), avaliação/interpretação da árvore.
**Critério de pronto:** a mesma regra escrita na sua mini-linguagem produz o resultado correto pra múltiplos valores de entrada diferentes (ex: idade 15 → negado, idade 20 → liberado), sem hardcoded — o interpretador de fato avalia a condição.

### 10. Agendador de tarefas com dependências
**O que fazer:** recebe uma lista de tarefas onde cada uma pode depender de outras (ex: "compilar" depende de "instalar dependências", que depende de "clonar repositório") e calcula a ordem correta de execução, detectando e reportando erro se existir uma dependência cíclica (A depende de B que depende de A).
**Conceitos:** representação de grafo dirigido, ordenação topológica, detecção de ciclo em grafo.
**Critério de pronto:** uma lista de tarefas sem ciclo gera uma ordem de execução válida (cada tarefa só aparece depois de todas as suas dependências), e uma lista com ciclo de propósito é detectada e rejeitada com uma mensagem que aponta quais tarefas formam o ciclo.

---

## Resumo de progressão

| # | Tema | Nível | Estrutura/conceito central |
|---|------|-------|------------------------------|
| 1 | Agenda de contatos | Médio | Arquivo + serialização |
| 2 | Validador CPF/CNPJ | Médio | String + aritmética |
| 3 | Markdown → HTML | Médio | Parsing + regex |
| 4 | Reserva de assentos concorrente | Médio | Concorrência/exclusão mútua |
| 5 | Gerador/verificador de senha | Médio | Aleatoriedade segura + regras |
| 6 | Compactador Huffman | Difícil | Árvore binária + heap + bits |
| 7 | Resolvedor de labirinto | Difícil | Grafo implícito + BFS/DFS |
| 8 | Mini banco chave-valor | Difícil | I/O + índice + log append-only |
| 9 | Interpretador de mini-linguagem | Difícil | Tokenização + parsing + AST |
| 10 | Agendador com dependências | Difícil | Grafo dirigido + ordenação topológica |

Cada linha da tabela = 2 projetos (1 em Go, 1 em TypeScript) + 1 bloco de comparação escrito por você ao final. Total: 20 implementações + 10 comparações.
