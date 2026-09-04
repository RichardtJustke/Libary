# Roteiro Mestre — Especialização Backend (Go + TypeScript/Node.js)

Roteiro multilinguagem: duas trilhas de backend independentes (Go e TypeScript/Node.js) organizadas no mesmo arquivo, no mesmo formato (tema → GG → XXL). A ideia de uso é intercalar — um projeto por semana, alternando de trilha — pra treinar os dois ecossistemas em paralelo sem enjoar do mesmo stack. Detalhes de como intercalar estão em "Como usar este roteiro mestre" no final do arquivo.

---

# TRILHA GO

## PARTE 1 — Projetos por Tema (fundação)

### 🔹 MENSAGERIA (10 projetos)

**1. Produtor/Consumidor RabbitMQ**
O que fazer: cria dois serviços Go separados rodando em processos distintos — `producer` expõe um endpoint HTTP que, ao ser chamado, monta uma mensagem JSON (ex: `{"pedido_id": 1, "valor": 99.90}`) e publica na fila `pedido.criado` usando a lib oficial `amqp091-go`; `consumer` abre uma conexão AMQP, declara a mesma fila, consome as mensagens e loga o conteúdo no console. Sobe o RabbitMQ localmente via `docker-compose` (imagem `rabbitmq:3-management` pra ter a UI de administração). Implementa ack manual (`msg.Ack(false)`) em vez de auto-ack, pra controlar exatamente quando a mensagem é considerada processada.
Objetivo: entender na prática os quatro conceitos-base de fila (exchange, queue, binding, ack/nack) e a diferença entre "a mensagem chegou" e "a mensagem foi processada com sucesso".
Critério de pronto: matar o processo do `consumer` no meio do processamento, subir de novo, e a mensagem que estava sendo processada não pode ter se perdido (ela volta pra fila porque não foi "acked").

**2. Dead Letter Queue e Retry**
O que fazer: evolui o projeto 1 fazendo o `consumer` falhar de propósito nas primeiras N tentativas de uma mesma mensagem (simula erro lançando exceção controlada, ex: baseado em um contador em memória por `pedido_id`). Configura uma Dead Letter Exchange (DLX) no RabbitMQ: quando uma mensagem é rejeitada (`Nack` com `requeue=false`) X vezes, ela deve cair automaticamente numa fila separada (`pedido.criado.dlq`). Implementa retry com backoff exponencial (ex: espera 1s, depois 2s, depois 4s antes de tentar de novo) usando uma fila intermediária com TTL por mensagem, ou uma lib de retry.
Objetivo: entender o que acontece quando processamento falha de verdade em produção — sem isso, mensagens problemáticas ficam reprocessando pra sempre (poison message) e travam a fila inteira.

**3. Fanout / Pub-Sub com múltiplos consumidores**
O que fazer: cria um evento (`usuario.cadastrado`) publicado uma única vez por um serviço de cadastro. Configura uma exchange do tipo `fanout` no RabbitMQ e cria três filas diferentes, cada uma vinculada (bind) à mesma exchange: `email-service` (simula envio de e-mail de boas-vindas), `analytics-service` (loga o evento numa "tabela" de métricas), `crm-service` (simula criação de registro no CRM). Cada consumer roda em processo Go separado, totalmente independente dos outros.
Objetivo: entender desacoplamento real — o publisher não precisa saber quem consome nem quantos consumidores existem; adicionar um quarto serviço no futuro não exige mudar uma linha do publisher.

**4. Migração RabbitMQ → Kafka**
O que fazer: pega o projeto 1 ou 3 e reimplementa a mesma lógica usando Kafka em vez de RabbitMQ, com `segmentio/kafka-go` (ou `confluent-kafka-go`). Sobe um cluster Kafka local via `docker-compose` (Kafka + Zookeeper ou modo KRaft). Cria um tópico com 3 partições. Publica mensagens usando uma chave (ex: `pedido_id` como key), garantindo que mensagens da mesma chave sempre caiam na mesma partição (e portanto mantenham ordem entre si).
Objetivo: sentir na prática a diferença de modelo — RabbitMQ é uma fila que "some" a mensagem depois de consumida (modelo de fila clássica); Kafka é um log distribuído onde a mensagem persiste e pode ser lida várias vezes por consumidores diferentes.

**5. Consumer Groups e Replay**
O que fazer: sobe dois processos `consumer` no mesmo consumer group Kafka, consumindo do tópico do projeto 4. Observa o Kafka dividir automaticamente as partições entre os dois consumers (paralelismo automático). Implementa um comando/flag de "replay": reseta o offset do consumer group pra um ponto anterior (ex: `--from-beginning` ou um offset específico) e reprocessa mensagens antigas.
Objetivo: entender os conceitos de consumer group (paralelismo automático de consumo), offset (ponteiro de onde cada consumer parou) e por que Kafka permite "voltar no tempo" pra reprocessar dados históricos — coisa que uma fila RabbitMQ tradicional não permite, já que a mensagem já foi descartada.

**6. NATS básico (produtor/consumidor)**
O que fazer: sobe um servidor NATS local (`docker run nats`). Um serviço publica um evento (`sensor.temperatura`) periodicamente (ex: a cada 2 segundos, simulando leitura de sensor) usando o client Go oficial (`nats.go`). Outro serviço assina (`Subscribe`) o mesmo assunto e loga o valor recebido. Testa desligar o subscriber por alguns segundos e ligar de novo, observando que as mensagens publicadas nesse intervalo se perderam (não há persistência por padrão).
Objetivo: sentir a diferença de modelo "fire-and-forget" (NATS puro, leve, baixíssima latência, mas sem garantia de entrega) versus fila com garantia de entrega (RabbitMQ/Kafka).

**7. NATS JetStream (persistência)**
O que fazer: evolui o projeto 6 ativando o JetStream (camada de persistência do NATS) — cria um "stream" que armazena as mensagens publicadas no assunto, com uma política de retenção (ex: por tempo ou por quantidade). Reimplementa o consumer usando um "consumer" do JetStream (pull ou push based) em vez da subscription simples. Testa: desliga o consumer, publica mensagens, sobe o consumer de novo — ele deve conseguir buscar as mensagens que perdeu.
Objetivo: entender a diferença entre NATS "puro" (at-most-once, sem garantia) e NATS com JetStream (at-least-once, com garantia de entrega e replay), e quando vale a pena pagar o custo extra de persistência.

**8. Saga coreografada com eventos**
O que fazer: monta um cenário de reserva de viagem com três serviços independentes: `flight-service`, `hotel-service`, `car-service`. Cada um escuta um evento de "iniciar reserva" e responde publicando `voo.reservado`/`voo.falhou`, `hotel.reservado`/`hotel.falhou`, etc. Implementa a lógica de compensação: se `hotel-service` publicar `hotel.falhou`, o `flight-service` deve estar assinando esse evento e, ao recebê-lo, cancelar automaticamente a reserva de voo que já tinha feito (publicando `voo.cancelado`). Não existe um orquestrador central — cada serviço reage de forma independente ao que os outros publicam.
Objetivo: entender o padrão Saga coreografada, muito usado quando uma transação de negócio precisa tocar múltiplos serviços/bancos que não podem compartilhar uma transação ACID única.

**9. Outbox Pattern**
O que fazer: implementa um serviço que precisa gravar uma mudança no Postgres E publicar um evento correspondente (ex: criar um pedido e publicar `pedido.criado`), mas de forma atômica — não pode acontecer de gravar no banco e falhar ao publicar, ou vice-versa, deixando o sistema inconsistente. Cria uma tabela `outbox` na mesma transação da escrita de negócio (o INSERT do pedido e o INSERT na outbox acontecem no mesmo `BEGIN/COMMIT`). Um processo separado (poller ou usando CDC/Debezium se quiser ir além) lê periodicamente a tabela outbox, publica de fato as mensagens pendentes no RabbitMQ/Kafka, e marca como publicadas.
Objetivo: resolver o problema clássico do "dual write" em sistemas distribuídos, onde gravar em dois sistemas diferentes (banco + fila) sem coordenação pode causar perda ou duplicação de eventos.

**10. Priorização de fila**
O que fazer: cria uma fila com mensagens de dois níveis de prioridade — alta (ex: pedido VIP) e baixa (ex: pedido normal). No RabbitMQ, usa o recurso nativo de "priority queue" (`x-max-priority` na declaração da fila) e publica mensagens com o campo `priority` setado. Testa publicando vários pedidos de prioridade baixa seguidos de um de prioridade alta, e confirma que o consumer processa o de alta prioridade primeiro mesmo tendo chegado por último.
Objetivo: entender como implementar roteamento por prioridade em sistemas de mensageria — cenário comum em filas de suporte, pagamentos VIP, ou processamento com SLA diferenciado.

---

### 🔹 KUBERNETES (11 projetos)

**1. Deploy + Rolling Update + Rollback**
O que fazer: escreve uma API Go simples com dois endpoints, `/health` (retorna 200 OK) e `/version` (retorna a versão embutida via `ldflags` no build). Empacota em uma imagem Docker multi-stage (stage de build compila o binário estático, stage final usa `scratch` ou `alpine` só com o binário). Cria um `Deployment` do Kubernetes com 2 réplicas e um `Service` do tipo ClusterIP na frente. Muda a tag da imagem pra uma versão nova e aplica (`kubectl apply`), observando o rolling update rodar pod por pod sem downtime (usa `kubectl rollout status` pra acompanhar). Depois, sobe de propósito uma versão que crasha no boot (ex: `os.Exit(1)` logo no `main`) e observa o Kubernetes travar o rollout automaticamente (readiness falha, não substitui todos os pods de uma vez); faz `kubectl rollout undo` pra voltar à versão anterior.
Objetivo: entender como o Kubernetes garante disponibilidade durante deploys e como reverter rápido quando algo dá errado em produção.

**2. ConfigMap + Secret**
O que fazer: pega a mesma API e faz a porta HTTP, a string de conexão de um banco (pode ser fake/mock) e uma "API key" virem de variáveis externas em vez de hardcoded. Cria um `ConfigMap` pra porta e string de conexão (não-sensível), e um `Secret` pra API key (sensível, base64). Injeta ambos como variáveis de ambiente no `Deployment`. Testa duas coisas: (a) mudar o valor no `ConfigMap` e aplicar de novo — o pod já existente enxerga a mudança sem reiniciar, ou precisa de restart manual? (b) o mesmo teste pro `Secret`. Testa também a alternativa de montar o ConfigMap como volume (arquivo) em vez de env var, e compara o comportamento de atualização.
Objetivo: entender as diferenças práticas entre injetar configuração via variável de ambiente (não atualiza sozinha, exige restart do pod) versus volume montado (pode atualizar em tempo real, dependendo de como a aplicação lê o arquivo).

**3. Probes e Self-Healing**
O que fazer: adiciona três endpoints na API: `/live` (liveness — sempre retorna 200 enquanto o processo estiver rodando), `/ready` (readiness — pode simular "não pronto" por um tempo no boot), e `/travar` (força a aplicação a travar de propósito, ex: entrando em loop infinito ou parando de responder ao HTTP). Configura `livenessProbe` e `readinessProbe` no manifesto do Deployment apontando pra esses endpoints com intervalos curtos (ex: `periodSeconds: 5`). Chama `/travar` num dos pods e observa o Kubernetes detectar a falha via liveness probe, matar o pod e subir um novo automaticamente, sem você precisar fazer nada manualmente.
Objetivo: entender a diferença entre liveness (quando falha, o Kubernetes mata e recria o pod) e readiness (quando falha, o Kubernetes só tira o pod da lista de endpoints do Service, sem matá-lo — útil pra warm-up ou dependência temporariamente indisponível).

**4. HPA sob carga real**
O que fazer: configura `resources.requests` (e idealmente `limits`) de CPU no Deployment — isso é obrigatório pro HorizontalPodAutoscaler funcionar baseado em CPU. Cria um `HorizontalPodAutoscaler` apontando pro Deployment, com um alvo de utilização de CPU (ex: 50%) e min/max réplicas (ex: 1 a 10). Gera carga real contra a API usando `k6` (script simples de load test) ou `hey` (`hey -z 60s -c 50 http://...`). Acompanha com `kubectl get hpa -w` o número de réplicas subindo conforme a CPU sobe, e descendo depois que a carga para (com o período de cooldown padrão).
Objetivo: ver autoscaling acontecendo de verdade sob carga real, não só ler a teoria — inclusive sentir o atraso natural entre "carga aumenta" e "novos pods ficam prontos".

**5. Ingress + múltiplos serviços**
O que fazer: cria dois serviços Go simples (`service-a` respondendo em `/api/a`, `service-b` em `/api/b`), cada um com seu próprio Deployment e Service. Instala um Ingress Controller no cluster (ex: `ingress-nginx` via Helm, se estiver usando minikube/kind). Cria um único recurso `Ingress` que roteia `/api/a` pro `service-a` e `/api/b` pro `service-b`, ambos acessíveis por um único ponto de entrada/host.
Objetivo: entender roteamento de camada 7 (baseado em path/host) dentro do cluster, evitando expor um LoadBalancer/NodePort por serviço.

**6. (bônus) Empacotar tudo com Helm**
O que fazer: pega os manifestos dos projetos 1 a 5 e transforma num Helm chart parametrizável — extrai valores variáveis (número de réplicas, tag da imagem, limites de recurso, hosts do Ingress) pra um `values.yaml`, usando templates Go (`{{ .Values.replicaCount }}`) nos manifestos. Testa subir tudo com um único `helm install meu-app ./chart` e trocar réplicas/imagem só editando o `values.yaml` e rodando `helm upgrade`.
Objetivo: aprender a ferramenta que praticamente toda empresa usa pra não reescrever/duplicar YAML pra cada ambiente ou serviço.

**7. Jobs e CronJobs**
O que fazer: cria um `Job` do Kubernetes que roda uma tarefa até completar e sai (ex: um programa Go que processa um arquivo grande simulado e termina com exit code 0). Configura `backoffLimit` pra controlar quantas vezes ele tenta de novo em caso de falha. Cria um `CronJob` separado que roda a mesma lógica todo dia às 3h da manhã (schedule `0 3 * * *`), simulando geração de um relatório diário.
Objetivo: entender a diferença entre workload de longa duração (Deployment, sempre rodando) e workload que tem começo, meio e fim (Job/CronJob), e quando usar cada um.

**8. Init Containers**
O que fazer: cria um Deployment onde o container principal só deve subir depois que uma tarefa preliminar terminar — configura um `initContainer` que, por exemplo, espera o banco estar acessível (loop de `pg_isready` ou tentativa de conexão) e/ou roda as migrations do banco (ex: `migrate` CLI) antes do container principal iniciar. O Kubernetes só inicia o container principal depois que todos os init containers terminarem com sucesso.
Objetivo: entender como garantir ordenação de inicialização dentro de um pod sem precisar de lógica de retry manual dentro da própria aplicação.

**9. Network Policies**
O que fazer: sobe dois serviços no cluster e um Postgres (pode ser via Deployment simples pra esse exercício). Configura o cenário onde um serviço (`service-a`) pode falar com o banco, mas outro (`service-b`) não deveria conseguir. Cria uma `NetworkPolicy` que restringe o tráfego de entrada do pod do Postgres, permitindo apenas tráfego vindo de pods com um label específico (ex: `app: service-a`). Testa com `kubectl exec` dentro do pod do `service-b` tentando `curl`/`nc` no banco e confirma que a conexão é bloqueada.
Objetivo: entender que, por padrão, tudo fala com tudo dentro de um cluster Kubernetes — segurança de rede real exige NetworkPolicy explícita.

**10. StatefulSet**
O que fazer: sobe um banco Postgres como `StatefulSet` em vez de `Deployment`, com um `PersistentVolumeClaim` associado a cada réplica (via `volumeClaimTemplates`). Observa que cada pod recebe um nome estável e previsível (`postgres-0`, `postgres-1`, ...), diferente do nome aleatório de um Deployment. Testa deletar o pod e confirma que ele sobe de novo com o mesmo nome e o mesmo volume de dados (os dados não se perdem).
Objetivo: entender por que aplicações com estado (bancos, filas com disco) precisam de identidade estável de pod e volume dedicado — algo que um Deployment comum não garante.

**11. Multi-ambiente com Kustomize**
O que fazer: pega os manifestos base de uma aplicação e organiza a estrutura de pastas do Kustomize (`base/` com os manifestos genéricos, `overlays/dev/` e `overlays/prod/` com patches específicos). No overlay de `dev`, configura 1 réplica e limites de recurso baixos; no de `prod`, configura mais réplicas e limites maiores, além de variáveis de ambiente diferentes. Aplica com `kubectl apply -k overlays/prod` e `kubectl apply -k overlays/dev` e compara a saída.
Objetivo: aprender a não duplicar o YAML inteiro pra cada ambiente, mantendo só as diferenças explícitas em patches.

---

### 🔹 AWS (10 projetos)

**1. Lambda + API Gateway**
O que fazer: escreve uma função Go simples (ex: recebe um JSON e retorna um cálculo ou consulta fake), compila com `GOOS=linux GOARCH=arm64` (Graviton, mais barato) usando o runtime customizado da AWS pra Go. Faz o deploy da função como Lambda (via console, SAM ou Terraform) e cria um API Gateway HTTP API na frente, expondo um endpoint público que invoca a Lambda.
Objetivo: sentir o modelo serverless puro — não existe servidor rodando 24/7, você paga só pela execução, e a aplicação escala e desliga sozinha.

**2. ECS Fargate + RDS**
O que fazer: pega a mesma API Go do Kubernetes (com `/health`) e empacota como imagem Docker, sobe num repositório ECR. Cria um cluster ECS com Fargate (sem gerenciar EC2/VM), define uma Task Definition e um Service com N réplicas. Cria uma instância RDS PostgreSQL (não local) e conecta a aplicação a ela via variável de ambiente com o endpoint do RDS, configurando corretamente o Security Group pra permitir a conexão só a partir das tasks do ECS.
Objetivo: entender container gerenciado em produção sem precisar administrar sistema operacional/VM, e a configuração de rede mínima (VPC, security groups) pra dois recursos AWS conversarem entre si com segurança.

**3. SQS produtor/consumidor**
O que fazer: reimplementa a lógica do projeto 1 de mensageria (producer/consumer), mas trocando o RabbitMQ self-hosted por uma fila SQS gerenciada da AWS. O producer usa o SDK Go da AWS (`aws-sdk-go-v2/service/sqs`) pra enviar mensagens; o consumer faz polling (`ReceiveMessage`) e deleta a mensagem depois de processar (`DeleteMessage`).
Objetivo: comparar na prática fila gerenciada (menos operação, menos controle fino sobre configurações internas) versus self-hosted (mais controle, mas você que cuida de escalar/atualizar/monitorar o broker).

**4. CloudWatch Logs + Alarme**
O que fazer: pega qualquer projeto anterior rodando na AWS e configura logs estruturados (JSON) sendo enviados pro CloudWatch Logs (automático se estiver em Lambda/ECS com o driver certo). Cria uma métrica de filtro que conta ocorrências de erro 500 nos logs, e configura um `CloudWatch Alarm` que dispara uma notificação (ex: via SNS pra e-mail) quando há "mais de 10 erros 500 em 5 minutos".
Objetivo: aprender observabilidade nativa do ecossistema AWS sem precisar subir Prometheus/Grafana quando o time/projeto é pequeno.

**5. S3 + processamento assíncrono**
O que fazer: cria um bucket S3 e permite upload de arquivos (ex: um CSV com uma lista de produtos). Configura um evento de notificação do S3 (`s3:ObjectCreated`) que dispara automaticamente uma função Lambda. A Lambda lê o arquivo recém-enviado, processa (ex: conta linhas, valida se as colunas esperadas existem, calcula alguma soma) e grava o resultado em outro lugar (DynamoDB ou outro objeto S3).
Objetivo: entender arquitetura orientada a eventos nativa da nuvem, sem precisar de um worker rodando continuamente esperando arquivo novo.

**6. DynamoDB + Lambda**
O que fazer: monta uma API serverless completa de CRUD (ex: itens de um catálogo) onde a Lambda em Go lida com `Create`, `Read`, `Update`, `Delete` gravando direto no DynamoDB (em vez de RDS). Modela a tabela pensando em partition key (ex: `item_id`) e, se fizer sentido, um sort key pra consultas por categoria/data.
Objetivo: sentir o modelo de banco NoSQL gerenciado e a importância do design de chave de partição — no DynamoDB, a modelagem de acesso vem antes da modelagem de dados (diferente do relacional).

**7. Step Functions**
O que fazer: orquestra um fluxo de múltiplas Lambdas usando o AWS Step Functions como orquestrador visual — por exemplo: `ValidarPedido` (Lambda) → `ProcessarPagamento` (Lambda) → `EnviarConfirmacao` (Lambda), com tratamento explícito de branch de erro (se o pagamento falhar, vai pra um estado de "pedido recusado" em vez de seguir o fluxo feliz).
Objetivo: entender orquestração centralizada de um fluxo com múltiplas etapas — contraponto direto ao padrão Saga coreografada (mensageria, projeto 8), onde não existe um "maestro" central.

**8. API Gateway com autenticação (Cognito ou JWT customizado)**
O que fazer: protege um endpoint do API Gateway exigindo um token válido — pode usar um Authorizer nativo integrado ao Amazon Cognito (usuário se autentica no Cognito e recebe um JWT) ou implementar um Lambda Authorizer customizado que valida um JWT próprio.
Objetivo: entender autenticação/autorização gerenciada na nuvem, incluindo o fluxo de emissão/validação de token sem você ter que implementar tudo manualmente.

**9. EventBridge**
O que fazer: um serviço publica um evento customizado (ex: `{"tipo": "pedido.criado", "valor": 150}`) no Amazon EventBridge. Cria múltiplas regras (`Rules`) que filtram por conteúdo do evento (ex: uma regra pra "valor > 100" dispara uma Lambda de alerta de VIP, outra regra genérica dispara uma Lambda de log).
Objetivo: entender roteamento de eventos baseado em conteúdo — um padrão muito usado em arquiteturas serverless pra desacoplar quem publica de quem reage, com regras declarativas em vez de código de roteamento manual.

**10. ElastiCache (Redis gerenciado)**
O que fazer: pega uma API que já tem cache local (ex: um `map` em memória ou cache in-process) e migra pra usar Amazon ElastiCache (Redis gerenciado). Ajusta o código pra usar um client Redis (`go-redis`) apontando pro endpoint do ElastiCache, configurando o Security Group corretamente.
Objetivo: entender cache gerenciado (menos operação, mas com latência de rede) versus cache in-process (mais rápido, mas não compartilhado entre réplicas da aplicação).

---

### 🔹 gRPC / COMUNICAÇÃO ENTRE SERVIÇOS (6 projetos)

**1. Conversão REST → gRPC**
O que fazer: pega uma API REST simples de CRUD (ex: usuários) e reescreve o contrato em Protobuf (`.proto` com mensagens `User`, `CreateUserRequest`, etc., e um serviço `UserService` com métodos `CreateUser`, `GetUser`, `UpdateUser`, `DeleteUser`). Gera o código Go com `protoc`/`buf` e implementa o mesmo CRUD como servidor gRPC, além de um cliente gRPC simples pra testar.
Objetivo: sentir a diferença de payload (binário compacto vs. JSON texto), geração de código automática a partir do contrato, e a garantia de tipos que o Protobuf força em ambos os lados (cliente e servidor sempre concordam no formato).

**2. Streaming server-side**
O que fazer: implementa um serviço gRPC com um método de streaming server-side (ex: `StreamCotacao`) que, uma vez chamado, mantém a conexão aberta e transmite uma nova cotação de preço a cada segundo pro cliente, sem o cliente precisar fazer nova requisição a cada vez.
Objetivo: entender um padrão de comunicação que REST tradicional não resolve bem (ficaria fazendo polling repetido); streaming resolve isso com uma única conexão de longa duração.

**3. Gateway REST→gRPC**
O que fazer: cria um serviço HTTP simples na frente (pode usar `grpc-gateway` ou implementar manualmente com Fiber/Gin) que recebe requisições REST do cliente externo e as traduz internamente pra chamadas gRPC contra o serviço do projeto 1.
Objetivo: entender por que empresas costumam expor REST/JSON pro público (mais simples pra clientes externos, navegadores, mobile) mas usam gRPC internamente entre seus próprios serviços (mais performático e tipado).

**4. Bidirectional streaming**
O que fazer: implementa um chat simples via gRPC com streaming bidirecional — cliente e servidor mandam mensagens continuamente na mesma conexão aberta, sem um "request/response" tradicional. Cada cliente conectado recebe as mensagens enviadas por outros clientes conectados no mesmo canal.
Objetivo: entender o padrão mais avançado de streaming gRPC, onde ambos os lados podem enviar dados a qualquer momento, de forma assíncrona um em relação ao outro.

**5. Interceptors (middleware gRPC)**
O que fazer: implementa um interceptor de logging (loga método chamado, duração, código de status de todas as chamadas gRPC) e um interceptor de autenticação (extrai um token do metadata da chamada e valida antes de deixar a chamada seguir pro handler real), aplicados globalmente no servidor gRPC.
Objetivo: entender como fazer cross-cutting concerns (log, auth, métricas) em gRPC — o equivalente conceitual ao middleware do Fiber/Gin no mundo REST.

**6. Service mesh básico (Linkerd ou Istio)**
O que fazer: sobe 2 serviços simples no Kubernetes que se comunicam via gRPC entre si, instala um service mesh leve (Linkerd é mais simples de começar) no cluster, e injeta o sidecar automaticamente nos pods. Observa mTLS sendo aplicado automaticamente entre os dois serviços (sem você ter escrito uma linha de código de criptografia) e testa retry automático configurado no mesh quando um dos serviços falha esporadicamente.
Objetivo: entender o que um service mesh resolve "de graça" na camada de infraestrutura (mTLS, retry, métricas de tráfego) que, sem ele, você teria que codar manualmente em cada serviço.

---

### 🔹 OBSERVABILIDADE (6 projetos)

**1. Prometheus + Grafana numa API**
O que fazer: instrumenta uma API Go com a lib `prometheus/client_golang`, expondo um endpoint `/metrics`. Cria pelo menos três métricas: um contador de requisições totais (com labels de rota e método), um histograma de latência por requisição, e um contador de taxa de erro (respostas 5xx). Configura o Prometheus pra fazer scraping desse endpoint (`prometheus.yml` com o target da API). Sobe o Grafana, conecta na fonte de dados Prometheus e monta um dashboard exibindo essas três métricas em painéis separados.
Objetivo: aprender a instrumentar código de verdade (onde colocar os contadores/histogramas no meio da lógica de negócio), não só instalar as ferramentas e olhar pra uma tela vazia.

**2. Tracing distribuído (2 serviços)**
O que fazer: monta um cenário onde o Serviço A recebe uma requisição e chama o Serviço B via HTTP ou gRPC pra completar o processamento. Instrumenta ambos com OpenTelemetry (SDK Go), propagando o contexto de trace entre eles (via headers HTTP ou metadata gRPC), e exporta os spans pro Jaeger. Injeta uma latência artificial aleatória (`time.Sleep` com valor randômico) dentro de um dos dois serviços, de propósito.
Objetivo: usar o Jaeger pra achar exatamente onde está o gargalo (qual span específico está demorando), praticando debugging real de sistema distribuído em vez de "adivinhar" onde está o problema.

**3. Logs estruturados + correlação**
O que fazer: pega o cenário do projeto anterior (2 serviços) e faz ambos logarem em formato JSON (usando `slog` ou `zap`/`zerolog`), incluindo o mesmo `trace_id` do OpenTelemetry em cada linha de log, propagado do Serviço A pro Serviço B junto com a requisição.
Objetivo: entender correlação de logs em ambiente distribuído — conseguir filtrar/buscar por um único `trace_id` e ver a linha do tempo completa de uma requisição específica passando pelos dois serviços, mesmo que os logs estejam em arquivos/serviços diferentes.

**4. RED Method (Rate, Errors, Duration)**
O que fazer: instrumenta uma API seguindo especificamente a metodologia RED — monta um dashboard com exatamente três painéis: taxa de requisições por segundo (Rate), taxa de erro em porcentagem (Errors), e distribuição de duração das requisições, geralmente com percentis p50/p95/p99 (Duration).
Objetivo: aprender uma metodologia real usada em produção pra monitorar serviços (bastante citada em times de SRE), em vez de espalhar métricas soltas sem critério do que realmente importa olhar primeiro.

**5. Alerting no Prometheus (Alertmanager)**
O que fazer: configura uma regra de alerta no Prometheus (ex: `rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m]) > 0.05` — taxa de erro acima de 5% nos últimos 5 minutos por 2 minutos seguidos) e configura o Alertmanager pra rotear esse alerta pra um canal de notificação (webhook simples, Slack, ou até um "e-mail fake" via SMTP local pra teste).
Objetivo: sair do modelo "só visualizar métricas quando alguém lembra de olhar" pra "ser avisado automaticamente" quando algo sai do esperado.

**6. Profiling com pprof**
O que fazer: escreve uma aplicação Go com um bug de performance proposital (ex: uma função que aloca um slice novo dentro de um loop sem necessidade, causando alocação excessiva de memória, ou uma goroutine leak). Ativa o pacote nativo `net/http/pprof` na aplicação e usa `go tool pprof` pra coletar e analisar um CPU profile e um heap profile, identificando exatamente qual função está consumindo mais recurso.
Objetivo: aprender a ferramenta nativa do Go pra achar problema de performance real dentro do próprio processo, complementando (não substituindo) o que Prometheus/Grafana mostram de fora.

---

### 🔹 BANCO DE DADOS (6 projetos)

**1. Query tuning**
O que fazer: gera 1M+ linhas fake numa tabela Postgres (script Go que faz inserts em lote, ou `pgbench` com um schema customizado). Escreve uma query propositalmente lenta (ex: filtro por uma coluna sem índice, com `LIKE` no meio de string, ou um `JOIN` sem índice na chave). Roda `EXPLAIN ANALYZE` pra ver o plano de execução real e o tempo gasto. Cria o índice apropriado (`CREATE INDEX`) e roda o `EXPLAIN ANALYZE` de novo, comparando o tempo antes/depois.
Objetivo: sair da teoria de "índice deixa mais rápido" e ver o número mudando de verdade, entendendo como ler um plano de execução (seq scan vs. index scan).

**2. Particionamento**
O que fazer: pega a mesma tabela grande do projeto anterior, mas recria ela particionada por data (ex: `PARTITION BY RANGE (created_at)`, uma partição por mês). Popula com o mesmo volume de dados distribuído ao longo de vários meses. Compara o tempo de uma query com filtro de data (ex: "pedidos do mês passado") antes e depois de particionar.
Objetivo: entender quando particionamento realmente ajuda (queries que já filtram naturalmente pela coluna de partição) e quando não ajuda nada (queries que cruzam todas as partições).

**3. Migração de schema zero-downtime**
O que fazer: simula adicionar uma coluna `NOT NULL` numa tabela que já está em "produção" (com tráfego simulado de leitura/escrita constante rodando em paralelo via script). Em vez de rodar um `ALTER TABLE ... ADD COLUMN ... NOT NULL` direto (que trava a tabela inteira em tabelas grandes), aplica a estratégia expand/contract: primeiro adiciona a coluna como nullable, depois roda um script que popula o valor pra todas as linhas existentes em lotes pequenos, e só então aplica a constraint `NOT NULL`.
Objetivo: entender por que um "ALTER TABLE" ingênuo pode derrubar produção (lock de tabela inteira) e como fazer a mesma mudança sem downtime perceptível.

**4. Réplica de leitura**
O que fazer: configura uma instância Postgres primária e uma réplica read-only (replicação streaming nativa do Postgres). Ajusta a aplicação pra mandar todas as escritas (INSERT/UPDATE/DELETE) pro primário e todas as leituras (SELECT) pra réplica, usando duas connection strings diferentes.
Objetivo: entender separação de carga de leitura/escrita — um padrão comum quando o volume de leitura é muito maior que o de escrita e o primário sozinho não aguentaria tudo.

**5. Full-text search**
O que fazer: implementa busca de texto (ex: buscar produtos por nome/descrição) usando os recursos nativos do Postgres — cria uma coluna `tsvector` gerada a partir dos campos de texto, um índice `GIN` sobre ela, e faz as buscas com `tsquery` (incluindo ranking de relevância com `ts_rank`).
Objetivo: aprender que nem toda busca de texto precisa de uma ferramenta externa como Elasticsearch — pra volumes moderados, o Postgres já resolve bem e com muito menos peça de infraestrutura pra manter.

**6. Connection pooling (PgBouncer)**
O que fazer: coloca o PgBouncer na frente de uma instância Postgres, configurado em modo `transaction pooling`. Gera um teste de carga que abre muitas conexões simultâneas contra o banco (ex: com `pgbench -c 200`), primeiro direto no Postgres e depois passando pelo PgBouncer, comparando o comportamento (erros de "too many connections" direto vs. estabilidade com o pooler).
Objetivo: entender por que uma aplicação não deve abrir conexão direta e descontrolada com o banco — o Postgres tem um limite de conexões simultâneas caro de aumentar, e um pooler resolve isso de forma transparente.

---

### 🔹 AUTENTICAÇÃO E SEGURANÇA (5 projetos)

**1. JWT do zero**
O que fazer: implementa geração e validação de JWT manualmente (usando `golang-jwt/jwt`, mas sem uma lib de auth completa por cima) — no login, gera um access token de curta duração e um refresh token de duração maior, ambos assinados (HMAC ou RSA). Implementa um middleware Fiber que extrai o token do header `Authorization`, valida assinatura e expiração, e injeta os dados do usuário no contexto da requisição. Implementa o endpoint de refresh que troca um refresh token válido por um novo access token.
Objetivo: entender exatamente o que uma lib de auth pronta faz por baixo dos panos, antes de simplesmente confiar numa caixa preta em produção.

**2. OAuth2 com provedor externo**
O que fazer: implementa login via Google ou GitHub usando o fluxo OAuth2 Authorization Code — a aplicação redireciona o usuário pro provedor, recebe um código de autorização de volta, troca esse código por um access token do provedor, e usa esse token pra buscar os dados básicos do usuário (nome, e-mail) e criar/logar a conta na sua própria base.
Objetivo: entender o fluxo de autenticação delegada a terceiros, bem diferente de implementar autenticação própria (projeto anterior) — inclui lidar com `state` (proteção contra CSRF) e o redirect callback.

**3. RBAC (Role-Based Access Control)**
O que fazer: modela usuários com papéis (`admin`, `editor`, `viewer`) numa tabela de relacionamento (usuário-papel, podendo ter mais de um). Cria um middleware que, dado o papel do usuário autenticado, valida se ele tem permissão pra acessar uma rota específica (ex: só `admin` pode deletar, `editor` e `admin` podem criar/editar, todos podem ler).
Objetivo: entender autorização granular — ir além de "está logado ou não" e controlar exatamente o que cada tipo de usuário pode fazer.

**4. Rate limiting por API key**
O que fazer: implementa rate limiting usando token bucket ou sliding window com Redis, mas a chave de controle é a API key do cliente (não o IP de origem) — cada cliente com uma API key própria tem seu próprio limite (ex: 100 requisições/minuto), controlado com `INCR` + `EXPIRE` no Redis ou um script Lua atômico.
Objetivo: entender controle de uso por cliente, o padrão comum em qualquer API pública que vende acesso por plano/quota.

**5. Criptografia de dados sensíveis em repouso**
O que fazer: identifica um campo sensível (ex: CPF) e implementa criptografia simétrica (AES-GCM) desse campo antes de gravar no Postgres — a coluna no banco guarda o valor criptografado (bytes), nunca o texto puro. Implementa a decriptação só no momento da leitura autorizada, com a chave de criptografia vindo de um cofre de segredos (ou variável de ambiente pra fins do exercício).
Objetivo: entender criptografia aplicada a dado em repouso (dentro do banco), complementando o que TLS já protege em trânsito (na rede).

---

### 🔹 TESTES E CI/CD (5 projetos)

**1. Testes unitários com mocks**
O que fazer: estrutura a camada de acesso a dados atrás de uma interface (`UserRepository` com métodos `Create`, `FindByID`, etc.), e implementa a lógica de negócio dependendo dessa interface, nunca do banco diretamente. Gera um mock da interface (`gomock` ou `testify/mock`) e escreve testes unitários da lógica de negócio configurando o comportamento esperado do mock (ex: "quando `FindByID` for chamado com X, retorna Y").
Objetivo: aprender a testar a lógica de negócio isoladamente, sem precisar de um banco real rodando, tornando os testes rápidos e determinísticos.

**2. Testes de integração com Testcontainers**
O que fazer: usa a lib Testcontainers-Go pra subir um container Postgres real automaticamente no início da suíte de testes de integração, roda as migrations reais contra ele, e testa as queries de verdade (não mockadas) contra esse banco efêmero, que é destruído ao final dos testes.
Objetivo: entender teste de integração confiável — testa o SQL real, os tipos reais do banco, sem os riscos de mockar mal uma query complexa.

**3. Contract testing com Pact**
O que fazer: modela um cenário com um serviço consumidor e um provedor de API. Do lado do consumidor, escreve um teste que define as expectativas de contrato (quais campos, quais status codes espera receber) e gera um arquivo de "pacto". Do lado do provedor, roda um teste que verifica o pacto contra a implementação real da API, sem precisar rodar os dois serviços juntos.
Objetivo: entender como evitar quebrar um consumidor de API quando o provedor evolui, sem depender de testes end-to-end manuais entre times.

**4. Pipeline CI/CD completo**
O que fazer: configura um workflow do GitHub Actions com os estágios: lint (`golangci-lint run`) → testes (`go test ./...`) → build de imagem Docker → push pra um registry (ex: GHCR ou ECR) → deploy automático em Kubernetes (via `kubectl apply` direto, ou de forma mais avançada via ArgoCD fazendo GitOps).
Objetivo: automatizar o caminho completo do código até produção, reduzindo passos manuais e erro humano no processo de release.

**5. Feature flags**
O que fazer: implementa toggle de funcionalidade em runtime — pode usar um serviço próprio simples (uma tabela/endpoint que retorna se uma flag está ativa) ou uma lib/serviço existente (ex: Unleash). A aplicação consulta a flag antes de executar um trecho de código novo, permitindo ativar/desativar a funcionalidade sem precisar de um novo deploy.
Objetivo: entender deploy contínuo desacoplado de release de funcionalidade — o código pode estar em produção "desligado" até que o time decida ativá-lo pra um grupo de usuários.

---

### 🔹 CQRS E EVENT SOURCING (3 projetos)

**1. Separação de leitura e escrita (CQRS básico)**
O que fazer: modela um domínio (ex: pedidos) onde os comandos de escrita gravam num modelo normalizado no Postgres (tabelas separadas de pedido, item, cliente), enquanto as consultas de leitura vão contra uma "view" desnormalizada (tabela ou view materializada já pronta pra exibição, com tudo junto).
Objetivo: entender por que, em cenários de leitura intensa e complexa, o modelo ideal pra escrever dados não é o mesmo ideal pra ler dados — e por que forçar os dois a serem o mesmo modelo às vezes complica ambos os lados.

**2. Event Sourcing simples**
O que fazer: modela uma conta bancária onde, em vez de gravar o saldo atual direto numa coluna, grava-se apenas os eventos que aconteceram (`Deposito{valor: 100}`, `Saque{valor: 30}`) numa tabela de eventos append-only. O saldo atual é calculado reconstruindo (replay) todos os eventos da conta em ordem e aplicando cada um sequencialmente.
Objetivo: entender o modelo onde o histórico completo é a fonte da verdade, não o estado atual — o que abre portas pra auditoria total e "viagem no tempo" pra qualquer ponto passado.

**3. Projeções materializadas**
O que fazer: evolui o projeto anterior criando um processo (worker) que consome os eventos conforme são gravados e mantém uma tabela separada de "saldo atual" já pré-calculada, atualizada incrementalmente a cada novo evento — em vez de recalcular tudo do zero a cada consulta de saldo.
Objetivo: entender como CQRS e Event Sourcing se combinam na prática: os eventos são a fonte da verdade (escrita), a projeção é otimizada pra leitura rápida.

---

### 🔹 GRAPHQL (3 projetos)

**1. API GraphQL básica (gqlgen)**
O que fazer: define um schema GraphQL simples (`type User { id, name, posts }`, `type Post { id, title, author }`) com queries (`users`, `post(id)`) e mutations (`createPost`). Usa o `gqlgen` pra gerar os resolvers a partir do schema e implementa a lógica de cada resolver.
Objetivo: primeiro contato com GraphQL em Go, comparando diretamente com o modelo REST que você já domina — em especial a ideia do cliente pedir exatamente os campos que quer.

**2. Resolvendo N+1**
O que fazer: cria de propósito o problema clássico de N+1 — uma query que busca uma lista de posts e, pra cada post individualmente, dispara uma query separada pra buscar o autor (N+1 queries no total pra N posts). Resolve implementando um DataLoader que agrupa (batch) todas as buscas de autor de uma "rodada" de resolvers numa única query `WHERE id IN (...)`.
Objetivo: entender o principal problema de performance específico de GraphQL (que não existe da mesma forma em REST) e a técnica padrão pra resolvê-lo.

**3. Subscriptions (GraphQL em tempo real)**
O que fazer: implementa uma subscription (`onPostCreated`) que notifica os clientes conectados em tempo real assim que um novo post é criado — por baixo, isso normalmente é implementado com uma conexão WebSocket mantida aberta entre cliente e servidor.
Objetivo: entender GraphQL além do modelo request/response tradicional, cobrindo o caso de dados que mudam e precisam ser empurrados pro cliente.

---

### 🔹 RATE LIMITING E MULTI-TENANCY (3 projetos)

**1. Multi-tenancy por schema**
O que fazer: num único banco Postgres, cria um schema separado por tenant (cliente) — ex: `tenant_acme`, `tenant_globex`, cada um com as mesmas tabelas replicadas dentro do seu schema. A aplicação decide dinamicamente qual schema usar em cada requisição (ex: baseado num subdomínio ou header identificando o tenant), ajustando o `search_path` da conexão.
Objetivo: entender uma estratégia de isolamento forte de dados entre clientes, mantendo um único banco físico.

**2. Multi-tenancy por linha (row-level)**
O que fazer: usa um schema único e compartilhado, mas toda tabela relevante ganha uma coluna `tenant_id`. Toda query da aplicação filtra explicitamente por `tenant_id`, e além disso configura Row-Level Security (RLS) nativo do Postgres, criando uma policy que reforça esse filtro no próprio banco (mesmo que a aplicação esqueça de filtrar, o banco bloqueia).
Objetivo: comparar as duas estratégias de multi-tenancy (schema separado vs. linha com RLS) e seus trade-offs de isolamento, complexidade operacional e performance.

**3. Cotas por tenant**
O que fazer: implementa um limite de uso diário por tenant (ex: 1000 requisições/dia), com um contador incrementado no Redis a cada requisição (`INCR` numa chave `quota:{tenant_id}:{data}`), configurado com expiração automática à meia-noite. Quando o contador ultrapassa o limite, a API retorna 429 (Too Many Requests) até o reset.
Objetivo: entender billing/cota aplicada de forma real (diferente de um rate limit genérico por segundo) — o tipo de controle usado por planos free/pago em produtos SaaS.

---

### 🔹 RESILIÊNCIA E CHAOS ENGINEERING (3 projetos)

**1. Circuit Breaker**
O que fazer: implementa um cenário onde o Serviço A chama o Serviço B via HTTP, usando uma lib de circuit breaker (`sony/gobreaker`). Configura o breaker pra abrir (parar de tentar chamar B) depois de X falhas seguidas, ficando num estado "aberto" por um tempo configurado, e depois passando pra um estado "half-open" que testa se B já voltou antes de fechar de novo.
Objetivo: entender como evitar que uma falha em cascata (B lento/fora do ar) derrube A também, ao parar de "bater a cabeça" numa dependência que já sabe que vai falhar.

**2. Bulkhead (isolamento de recursos)**
O que fazer: limita explicitamente o número de goroutines/conexões simultâneas que um serviço pode usar pra chamar uma dependência externa lenta (ex: usando um semáforo/worker pool com tamanho fixo), de forma que, mesmo que essa dependência trave completamente, o resto do serviço continue tendo recursos livres pra atender outras requisições.
Objetivo: entender isolamento de falha por compartimento — o nome vem de compartimentos estanques de navio, onde um alagamento não afunda o navio inteiro.

**3. Chaos Engineering básico**
O que fazer: usa uma ferramenta de chaos (Chaos Mesh, se estiver no Kubernetes) ou um script simples que mata pods/processos aleatoriamente em ambiente de teste, enquanto um teste de carga roda em paralelo. Observa se o sistema se recupera sozinho (probes reiniciando pods, réplicas cobrindo a ausência, circuit breaker evitando cascata).
Objetivo: validar resiliência experimentalmente, em vez de assumir "deveria funcionar" só porque a arquitetura foi desenhada com essas proteções.

---

### 🔹 BUILD YOUR OWN X — fundamentos de sistemas (20 projetos)

Filosofia diferente dos temas acima: não simula arquitetura de empresa — o objetivo é entender como as ferramentas que você já usa funcionam por dentro (Feynman: "o que eu não consigo construir, eu não entendo").

**Alta relevância pra entrevista técnica de sistemas:**

**1. Redis from Scratch**
O que fazer: implementa um servidor que aceita conexões TCP e entende o protocolo RESP do Redis, suportando pelo menos os comandos `GET`, `SET`, `DEL`, `EXPIRE`. Guarda os dados em memória usando um `map` protegido por `sync.Mutex` (ou, pra ir além, sharding de vários mapas com mutexes separados pra reduzir contenção). Implementa expiração de chave (TTL) rodando uma goroutine de limpeza periódica em background que varre e remove chaves expiradas.
Objetivo: entender por que Redis é rápido (tudo em memória, protocolo simples de parsear) e o que "TTL" realmente significa por dentro (não é mágica, é uma goroutine limpando de tempos em tempos).
Critério de pronto: conectar com o `redis-cli` de verdade apontando pro seu servidor e os comandos funcionarem normalmente.

**2. Database from Scratch (B+Tree → SQL)**
O que fazer: segue um guia estruturado em etapas — primeiro implementa uma B+Tree persistida em disco (não só em memória, com paginação de blocos e serialização), depois adiciona por cima um parser SQL simples que suporta pelo menos `SELECT` e `INSERT` básicos, traduzindo pra operações na B+Tree.
Objetivo: entender como um banco relacional organiza dados em disco pra busca eficiente — isso é exatamente o que fundamenta o que você vê quando roda `EXPLAIN ANALYZE` no Postgres (índice B-tree é o mesmo conceito).
Critério de pronto: inserir 100 mil registros e fazer uma busca por chave em tempo logarítmico, não linear (comprova medindo o tempo de busca conforme o volume cresce).

**3. Container em menos de 100 linhas de Go**
O que fazer: usa syscalls do Linux diretamente (namespaces via `unshare`/`clone` flags, cgroups escrevendo em `/sys/fs/cgroup`, `chroot` pra isolar o filesystem) pra isolar um processo, sem usar Docker nem nenhuma lib de containerização.
Objetivo: entender o que o Docker faz por baixo dos panos — isolamento de processo, filesystem e rede não é mágica, são primitivas do kernel Linux que qualquer programa pode usar diretamente.
Critério de pronto: rodar um processo isolado que, ao listar processos (`ps`), não enxerga os processos do sistema host, só os dele mesmo.

**4. Load Balancer simples**
O que fazer: escreve um servidor HTTP que recebe requisições e distribui entre N backends configurados, começando com round-robin simples (contador que alterna entre os backends), e depois adiciona health check periódico (faz um ping/`GET /health` em cada backend a cada X segundos e remove da rotação os que não respondem).
Objetivo: entender o que um load balancer de verdade faz — não é só "distribuir tráfego", é também detectar backend fora do ar e parar de mandar tráfego pra ele automaticamente.
Critério de pronto: derrubar um dos backends de propósito (matar o processo) e o load balancer parar de rotear pra ele automaticamente, sem erro pro cliente final.

**Relevância média — rede e concorrência:**

**5. Cliente BitTorrent**
O que fazer: implementa o parser do arquivo `.torrent` (formato bencode), conecta no tracker pra obter a lista de peers, implementa o handshake do protocolo BitTorrent com os peers, e troca mensagens pra baixar pedaços (pieces) do arquivo, verificando o hash de cada pedaço recebido.
Objetivo: prática pesada de protocolo binário, concorrência real (baixar de múltiplos peers ao mesmo tempo usando goroutines) e I/O de rede de baixo nível.
Critério de pronto: baixar um arquivo real via torrent usando só o seu próprio client, do início ao fim.

**6. Lexical Scanning em Go**
O que fazer: implementa um lexer (tokenizador) que recebe uma string de entrada e a quebra em tokens tipados (número, identificador, operador, etc.), seguindo a mesma abordagem que o próprio pacote `text/template` do Go usa internamente (inclusive a técnica de state functions do Rob Pike).
Objetivo: entender a primeira fase de qualquer parser/compilador — útil se algum dia você precisar escrever uma DSL própria, ou só pra entender por que um erro de sintaxe é reportado exatamente do jeito que é.
Critério de pronto: tokenizar corretamente uma linguagem simples inventada por você (ex: expressões de uma calculadora).

**7. Motor de Regex from Scratch**
O que fazer: implementa um motor de regex simples suportando `.` (qualquer caractere), `*` (zero ou mais), `+` (um ou mais) e grupos básicos, construindo um autômato finito (NFA) a partir do padrão e simulando ele contra a string de entrada, sem usar o pacote `regexp` do Go.
Objetivo: entender NFA/DFA (autômatos finitos) — a base teórica por trás de qualquer motor de busca de padrão, incluindo o próprio `regexp` que você usa no dia a dia.
Critério de pronto: seu motor bater exatamente com o resultado do `regexp` padrão do Go pra um conjunto de casos de teste que você mesmo define.

**Curiosidade / fundamentos de CS (fazer por último, ou nos finais de semana):**

**8. Blockchain em Go**
O que fazer: implementa uma estrutura de blocos encadeados por hash (cada bloco guarda o hash do anterior), com um mecanismo simples de prova de trabalho (proof-of-work — encontrar um nonce que faça o hash do bloco começar com N zeros), e uma função de validação que percorre a cadeia inteira conferindo se os hashes batem.
Objetivo: entender o conceito de blockchain sem o hype em volta — na essência é só estrutura de dados encadeada + hash + um mecanismo simples de consenso.
Critério de pronto: duas cópias independentes do seu blockchain concordarem sobre qual é a cadeia válida quando uma delas tenta "trapacear" (inserir um bloco inválido).

**9. Code Your Own Blockchain em menos de 200 linhas**
O que fazer: refaz a mesma lógica do projeto anterior de forma bem mais enxuta e direta, num único arquivo, priorizando simplicidade sobre robustez.
Objetivo: repetir o aprendizado do projeto 8 de forma compacta — bom como revisão rápida do conceito depois de já ter feito a versão completa.

**10. Multilayer Perceptron em Go**
O que fazer: implementa uma rede neural simples do zero — forward pass (multiplicação de matrizes + função de ativação, ex: sigmoid) e backpropagation (cálculo de gradiente e atualização de pesos), sem usar nenhum framework de ML.
Objetivo: entender o que acontece matematicamente quando um modelo é treinado, sem depender de PyTorch/TensorFlow escondendo os detalhes.
Critério de pronto: sua rede aprender a resolver o problema XOR (o teste clássico que prova que a rede consegue aprender uma função não-linear).

**11. Neural Net from Scratch (variação)**
O que fazer: implementa o mesmo conceito do projeto anterior, mas com uma estrutura de código diferente (ex: orientado a camadas como objetos separados, em vez de uma implementação monolítica).
Objetivo: reforçar o aprendizado com uma segunda perspectiva de implementação, comparando as duas abordagens de código.

**12. Artificial Neural Network (terceira variação)**
O que fazer: implementa uma terceira vez o mesmo conceito de rede neural simples, seguindo uma explicação/estrutura diferente das duas anteriores.
Objetivo: fixar o conceito de vez, útil se as duas primeiras tentativas não deixaram tudo claro.

**13. Games With Go [série em vídeo]**
O que fazer: acompanha a série construindo um jogo simples (geralmente 2D) do zero em Go, implementando loop de jogo, captura de entrada do usuário e renderização básica na tela.
Objetivo: prática de loop de jogo e renderização — pouco ligado a backend, mas bom pra variar o tipo de trabalho e é divertido.
Critério de pronto: ter um jogo jogável, mesmo que simples.

**14. CLI lolcat**
O que fazer: recria o clássico `lolcat`, que lê texto da entrada padrão e imprime aplicando um gradiente de cor arco-íris usando ANSI escape codes no terminal.
Objetivo: primeiro contato com manipulação de terminal via escape codes — projeto leve e rápido.
Critério de pronto: rodar `echo "oi" | seu-lolcat` e ver a cor aplicada corretamente.

**15. CLI cowsay**
O que fazer: recria o `cowsay` — recebe um texto por argumento ou stdin e desenha uma vaca ASCII com um balão de fala contendo o texto.
Objetivo: mesma ideia do anterior, com foco em parsing de argumento de linha de comando.
Critério de pronto: funcionar de forma equivalente ao `cowsay` original.

**16. CLI fortune clone**
O que fazer: recria o `fortune` — mantém uma lista de frases num arquivo, sorteia uma aleatoriamente a cada execução e imprime no terminal.
Objetivo: prática de leitura de arquivo, geração de número aleatório e estruturação simples de uma CLI.
Critério de pronto: rodar várias vezes e ver frases diferentes sendo sorteadas.

**17. Terminal Emulator em 100 linhas de Go**
O que fazer: implementa um emulador de terminal básico que interpreta um subconjunto de ANSI escape codes (posicionamento de cursor, cor) e desenha o resultado numa "tela" (pode ser um buffer de caracteres simples).
Objetivo: entender como um terminal "desenha" o que aparece na tela a partir de uma sequência de bytes recebidos de um processo.
Critério de pronto: rodar um comando simples (ex: `ls`) dentro do seu emulador e ver a saída correta na tela.

**18. Shell simples em Go**
O que fazer: implementa um shell básico que lê um comando digitado, faz `fork`/`exec` do processo correspondente (resolvendo o binário via `PATH`), espera terminar e imprime a saída. Adiciona suporte a comandos built-in (`cd`) e a um pipe simples entre dois comandos (`ls | grep algo`).
Objetivo: entender o que acontece entre você digitar um comando no terminal e ele efetivamente rodar — processo, variável `PATH`, redirecionamento de saída.
Critério de pronto: seu shell rodar `ls`, `cd` e um pipe simples corretamente.

**19. Visualizador de contribuições Git**
O que fazer: lê o histórico de commits de um repositório `.git` local (usando a lib `go-git` ou parseando os objetos do Git diretamente) e gera uma visualização tipo "heatmap" de contribuições, parecida com a do perfil do GitHub.
Objetivo: prática de manipular dados reais de um formato conhecido (o formato interno do Git), com um resultado que você pode realmente usar depois no seu dia a dia.
Critério de pronto: rodar no seu próprio repositório e ver o heatmap gerado bater com o real do GitHub.

**20. Database em 45 passos**
O que fazer: segue uma trilha de exercícios pequenos e incrementais (estilo TDD), cada um adicionando uma peça de um banco de dados simples, com testes prontos validando cada etapa antes de seguir pra próxima.
Objetivo: alternativa mais "passo a passo mastigado" ao projeto 2 (B+Tree → SQL), boa se você quiser algo mais guiado em vez de um guia mais aberto.
Critério de pronto: completar os 45 passos com os testes de cada etapa passando.

---

### 🔹 MULTIPLATAFORMA (React/Next.js + Go + Tauri + Capacitor) — 6 projetos

Cada projeto aqui tem uma razão real pra combinar as quatro ferramentas — o objetivo é evitar usar Tauri/Capacitor só "porque dá pra fazer", e sim porque a plataforma nativa resolve algo que web sozinha não resolve.

**1. Vault Local — Gerenciador de senhas com sync criptografado**
O que fazer: constrói um gerenciador de senhas estilo Bitwarden/1Password com arquitetura zero-knowledge — a criptografia end-to-end acontece inteiramente no cliente (deriva a chave mestra do usuário com Argon2 ou PBKDF2, cifra cada item com AES antes de enviar). No **Next.js**, monta a interface do cofre, gerador de senhas e formulários de login/cadastro. No **Tauri**, implementa o app desktop com integração ao keychain nativo do SO (Keychain no macOS, Credential Manager no Windows) via plugin Rust, um atalho global de teclado pra abrir o cofre rapidamente, e auto-lock automático após período de inatividade. No **Capacitor**, implementa autenticação biométrica (`@capacitor-community/biometric-auth`) pra desbloquear o cofre no celular, e uma versão simplificada de autofill de senhas. O **backend Go** só armazena blobs já criptografados (nunca decripta nada), gerencia sync entre dispositivos e autenticação de conta.
Objetivo: mostrar domínio de criptografia aplicada (AES, derivação de chave), segurança de dados sensíveis e arquitetura zero-knowledge — temas que pesam bastante em entrevista técnica.

**2. Fluxo — App de hábitos com widgets nativos e estatísticas**
O que fazer: constrói um rastreador de hábitos diários com gráficos de progresso e streaks. No **Next.js**, monta o dashboard principal, telas de configuração de hábito e os gráficos (Recharts ou D3). No **Tauri**, implementa um ícone na bandeja do sistema mostrando o progresso do dia, notificações nativas de lembrete, e uma mini-janela "always on top" pra marcar hábitos rapidamente sem abrir o app inteiro. No **Capacitor**, implementa um widget de tela inicial de verdade — escrevendo um plugin nativo customizado (Swift no iOS, Kotlin no Android) chamado via bridge do Capacitor, além de notificações locais agendadas. O **backend Go** expõe API de sync entre dispositivos, calcula streaks e estatísticas agregadas, e um endpoint de "insights" simples (ex: identificar o dia da semana com maior taxa de conclusão).
Objetivo: demonstrar capacidade de ir além do bridge padrão do Capacitor e escrever plugin nativo de verdade — algo raro em portfólio júnior/pleno e que chama atenção de quem revisa.

**3. Ponto Certo — Sistema de controle de ponto para freelancers/times pequenos**
O que fazer: constrói um app de registro de horas trabalhadas com geolocalização opcional e relatório em PDF, com um "modo chefe" pra aprovar horas de uma equipe pequena. No **Next.js**, monta o dashboard de relatórios, tela de aprovação de horas e exportação. No **Tauri**, implementa o app desktop pra quem trabalha home office bater ponto sem abrir navegador, com detecção de idle time (tempo sem mexer no mouse/teclado, via API do sistema acessada pelo lado Rust). No **Capacitor**, implementa o app mobile pra quem trabalha em campo, usando GPS (`@capacitor/geolocation`) pra confirmar o local do check-in. O **backend Go** implementa as regras de negócio (cálculo de horas extras, banco de horas acumulado), geração de relatório em PDF (`gofpdf` ou similar) e uma API multi-tenant, permitindo várias empresas diferentes usando o mesmo backend isoladamente.
Objetivo: montar um projeto com cara de produto SaaS real — modelagem de domínio de negócio (não um CRUD genérico), multi-tenancy de verdade e geração de relatório, bom pra quem quer vaga mais backend-heavy.

**4. Nota Rápida — Editor de notas markdown com sync tipo Git**
O que fazer: constrói um editor de notas em markdown parecido com Obsidian/Notion (mais simples), com sincronização entre dispositivos que mantém histórico de versões, como um "git simplificado" pra texto. No **Next.js**, monta o editor markdown com preview ao vivo, busca full-text e um grafo visual de links entre notas. No **Tauri**, dá acesso direto ao sistema de arquivos — as notas ficam salvas como arquivos `.md` reais na pasta do usuário (não escondidas num banco), com integração pra abrir num editor externo se o usuário quiser. No **Capacitor**, monta uma versão mobile leve, focada em captura rápida de nota e leitura, com sync em background. O **backend Go** implementa o motor de versionamento (diff/merge de texto) e a resolução de conflitos quando duas edições acontecem offline em dispositivos diferentes, além de um endpoint de busca (full-text search do Postgres ou indexação própria).
Objetivo: o destaque técnico aqui é o algoritmo de diff/merge de texto e resolução de conflito — mostra domínio de estrutura de dados e lógica de sincronização distribuída, não só telas bonitas.

**5. Radar de Preços — Monitor de preços de produtos com alertas**
O que fazer: constrói um app que monitora preços de produtos em e-commerces (via scraping ou API pública, quando disponível) e avisa o usuário quando o preço cai, com histórico em gráfico. No **Next.js**, monta o dashboard com lista de produtos monitorados e gráfico de histórico de preço. No **Tauri**, implementa um app desktop que roda em background (system tray) fazendo scraping periódico local, útil pra quem não quer depender 100% de um servidor externo. No **Capacitor**, implementa notificação push mobile quando o preço cai (integração com Firebase Cloud Messaging). O **backend Go** implementa workers/cron jobs usando goroutines e worker pools pra fazer scraping periódico de forma concorrente e eficiente, além de API de histórico de preços e do sistema de alertas.
Objetivo: excelente pra mostrar domínio de concorrência em Go (goroutines, channels, worker pools) combinado com integração de push notification — combo bastante presente em vagas de backend Go.

**6. Sala de Espera — App de fila/agendamento para pequenos negócios**
O que fazer: constrói um sistema onde o dono de um negócio (barbearia, clínica, oficina) gerencia uma fila de atendimento em tempo real, e o cliente acompanha sua posição na fila pelo celular sem precisar ficar fisicamente no local. No **Next.js**, monta o painel do estabelecimento (gerenciar fila, chamar o próximo) e uma tela pública de status da fila. No **Tauri**, implementa um app desktop pro balcão/recepção, pensado pra rodar numa tela dedicada em modo quiosque, com som de notificação quando alguém entra na fila. No **Capacitor**, implementa o app do cliente, recebendo notificação push quando está próximo de ser chamado. O **backend Go** implementa a atualização em tempo real da fila via WebSocket (`gorilla/websocket`) — todo mundo conectado vê a posição mudar instantaneamente — além da API de agendamento.
Objetivo: foco forte em tempo real (WebSockets em Go) e é um projeto fácil de explicar pra um recrutador não-técnico ("resolve uma dor real de pequenos negócios") — bom equilíbrio entre profundidade técnica e clareza de propósito de produto.

---

### 🔹 BANCO DE DADOS + AWS — trilha aplicada por nível de complexidade (6 projetos)

Trilha diferente da seção "AWS" e "Banco de Dados" acima: aqui cada projeto combina modelagem de dados e infraestrutura AWS no mesmo exercício, numa progressão de complexidade, e sempre com a orientação de "quebrar de propósito" depois de pronto (simular volume alto, derrubar instância e restaurar backup, calcular custo real com a AWS Pricing Calculator).

**Nível 1 — Fundamentos sólidos**

**1. Sistema de gestão de biblioteca/estoque com PostgreSQL + RDS**
O que fazer: modela o banco você mesmo do zero, pensando em normalização, chaves estrangeiras e índices (ex: livros, exemplares, empréstimos, usuários). Sobe o banco no Amazon RDS (não local), configurando VPC, Security Group (liberando acesso só da aplicação) e parâmetros de conexão. Implementa um CRUD completo com uma API simples.
Objetivo: aprender modelagem relacional na prática, além dos fundamentos de RDS, IAM e Security Groups que toda aplicação AWS usa.

**2. Encurtador de URLs**
O que fazer: modela uma tabela simples (código curto → URL de destino), mas pensando desde já em índice e performance sob muitos acessos de leitura. Faz o deploy como API em Lambda + API Gateway, com o banco em DynamoDB (ou RDS, pra comparar as duas abordagens) e um cache com ElastiCache/Redis na frente das leituras mais frequentes.
Objetivo: aprender arquitetura serverless, a diferença prática entre DynamoDB (NoSQL) e RDS (SQL) pra esse tipo de carga, e cache gerenciado.

**Nível 2 — Complexidade real**

**3. Plataforma de e-commerce simplificada**
O que fazer: modela um banco relacional com várias entidades relacionadas de verdade (usuários, produtos, pedidos, pagamentos, itens de pedido). Força-se a lidar com transações ACID (um pedido só é confirmado se o pagamento e a baixa de estoque acontecerem juntos) e concorrência (simula dois pedidos ao mesmo tempo tentando comprar o último item do estoque, usando lock otimista ou pessimista). Faz o deploy em EC2 ou ECS, RDS em modo Multi-AZ (alta disponibilidade), S3 pra imagens de produto e CloudFront como CDN na frente.
Objetivo: aprender transações e locks na prática, além de escalabilidade e arquitetura multi-camada real na AWS.

**4. Sistema de analytics/dashboard com dados em tempo real**
O que fazer: implementa ingestão de eventos simulados (ex: cliques de usuário) via Kinesis ou SQS. Armazena os eventos processados em DynamoDB (pra consulta pontual) ou Redshift (pra consulta analítica agregada).
Objetivo: entender a diferença entre banco transacional (OLTP, otimizado pra muitas escritas/leituras pontuais) e banco analítico (OLAP, otimizado pra agregações sobre grandes volumes), além de um primeiro contato com streaming de dados.

**Nível 3 — Produção de verdade**

**5. Réplica de rede social simples (feed, curtidas, comentários)**
O que fazer: modela o domínio pensando em escala desde o início (ex: como você faria sharding se tivesse milhões de usuários, mesmo sem implementar o sharding de fato — só já deixando a modelagem preparada). Usa RDS com read replicas pra separar leitura de escrita, e cache do feed com Redis (ElastiCache), invalidando o cache corretamente quando um novo post/curtida acontece.
Objetivo: aprender replicação, cache aplicado a um caso real (feed que muda com frequência), otimização de queries e monitoramento via CloudWatch.

**6. Pipeline de dados completo (ETL)**
O que fazer: extrai dados de uma API pública qualquer, transforma esses dados (limpeza, normalização, agregação) e carrega o resultado num Data Warehouse. Usa S3 como data lake (dados brutos), AWS Glue pra o job de ETL, e Redshift ou Athena pra consulta final sobre os dados já tratados.
Objetivo: aprender arquitetura de dados moderna (data lake + ETL + data warehouse), incluindo decisões de particionamento de dados e o trade-off de custo entre armazenamento e consulta.

**Dica de "quebrar de propósito" (vale pros 6 projetos acima):** depois de cada projeto pronto, simule 10 mil registros e veja o que trava, derrube a instância de propósito e pratique restaurar de um backup, e calcule quanto custaria rodar aquilo com tráfego real de produção usando a AWS Pricing Calculator.

---

### 🔹 LLM / AGENTES DE IA (integração + treino + produção) — 6 projetos

Trilha que mistura três frentes que normalmente ficam separadas: integração de LLM, criação/treino de modelo, e stack de produção de verdade. Toda a orquestração, serving e lógica de agente é feita 100% em Go; só o treino em si (fine-tuning) exige Python/PyTorch, isolado num passo específico. Ordem sugerida de aprendizado: **1 → 2 → 6 → 3 → 4 → 5**.

**1. Agente de function-calling com modelo estilo Hermes**
O que fazer: monta um orquestrador em Go (usando `net/http` ou Fiber) que implementa o loop de agente estilo ReAct — recebe uma pergunta do usuário, monta o prompt pro modelo, faz o parsing da saída estruturada em JSON (identificando se o modelo pediu pra chamar uma "tool"), executa a tool localmente (ex: uma função de calculadora, busca em um mock de banco de dados) e devolve o resultado pro modelo continuar o raciocínio até dar a resposta final. Roda um modelo Hermes (Nous Research) localmente via Ollama ou vLLM. Usa Postgres pra guardar a memória/histórico da conversa. Empacota tudo com Docker Compose.
Objetivo: aprender orquestração de agentes (tool use, loop de raciocínio, parsing de saída estruturada) de forma transparente — os modelos Hermes são treinados especificamente pra function-calling em JSON, então dá pra entender exatamente como o parsing de tool calls funciona por baixo do capô, muito mais visível do que usar direto a API da OpenAI/Anthropic.

**2. RAG pipeline com serving em Go**
O que fazer: implementa a etapa de ingestão/chunking de documentos em Python (usando langchain ou implementação própria), gerando embeddings e armazenando num banco vetorial — Qdrant é uma boa escolha por ter SDK Go nativo. A API de consulta (recebe a pergunta, gera embedding da pergunta, busca os chunks mais relevantes no banco vetorial, monta o prompt com contexto recuperado e chama o LLM) é implementada 100% em Go, com o LLM rodando via Ollama.
Objetivo: entender embeddings, chunking, retrieval e como servir tudo isso com baixa latência; dá pra comparar diretamente a latência de implementar o retrieval em Go versus Python, um exercício ótimo de "produção de verdade" versus "prototipagem".

**3. Fine-tuning de um modelo pequeno + servir em produção**
O que fazer: treina um adapter LoRA/QLoRA em Python (usando Unsloth ou a lib PEFT) sobre um modelo pequeno (ex: Llama 3.2 1B ou 3B) com um dataset específico de domínio. Exporta o resultado pro formato GGUF e serve via `llama.cpp` (rodando como servidor local). O backend Go conversa com esse servidor via HTTP, funcionando como camada de API/negócio por cima do modelo treinado.
Objetivo: sair de "só integrar API de terceiro" pra "eu de fato treinei isso" — é o projeto que fecha o ciclo de criação e treino de modelo; sem ele, os outros projetos da trilha são só integração.

**4. Sistema multi-agente (tipo mini AutoGPT) em Go**
O que fazer: implementa a orquestração de múltiplos agentes especializados conversando entre si (ex: um agente "pesquisador" que busca informação, um agente "crítico" que avalia a resposta do pesquisador, um agente "executor" que age com base no que foi decidido), usando goroutines e channels pra rodar os agentes em paralelo. Cada agente pode usar um modelo Hermes diferente, ou o mesmo modelo com prompts de sistema diferentes. Usa Redis como fila de mensagens entre os agentes.
Objetivo: aproveitar o ponto forte do Go (concorrência) pra orquestrar múltiplos agentes de forma muito mais natural do que seria em Python puro, sem depender de frameworks pesados de orquestração.

**5. Observabilidade/avaliação de LLM (LLM-as-judge)**
O que fazer: implementa em Go a coleta de traces de todas as chamadas ao LLM (latência, quantidade de tokens de entrada/saída, custo estimado por chamada), expõe essas métricas num dashboard simples (Grafana + Prometheus). Implementa um job separado que usa um segundo LLM como "juiz" pra avaliar a qualidade das respostas do primeiro (ex: dando uma nota de 1 a 5 com base em critérios definidos no prompt do juiz).
Objetivo: aprender a parte "chata" mas essencial de produção com LLM — avaliação contínua, logging e tracing — que a maioria dos projetos de portfólio ignora completamente.

**6. Fine-tuning de embeddings próprios + busca semântica**
O que fazer: treina um modelo de encoder (`sentence-transformers`) em Python sobre um dataset específico de domínio (mais simples de treinar do que um LLM generativo completo). Exporta o modelo treinado pro formato ONNX e roda a inferência de embeddings direto em Go usando `onnxruntime-go`, sem precisar de Python rodando em produção pra gerar embeddings.
Objetivo: entender treino de encoder model (mais acessível que treinar um LLM generativo) e como fazer a inferência de embeddings de forma leve, embutida na própria aplicação Go, sem dependência de um serviço Python separado em produção.

---

## PARTE 2 — Projetos XXL (agregam quase tudo) — Trilha Go

### XXL 1 — Plataforma de Apostas ao Vivo
O que construir: `events-service` (Go/Fiber) recebe atualização de placar de jogos e publica evento no Kafka; `odds-service` consome o evento, recalcula odds com lógica simples e publica novo evento; `bets-service` recebe apostas dos usuários (Postgres), validando contra as odds atuais; `notification-service` consome eventos de resultado e "notifica" (log ou webhook fake) quem apostou. Um API Gateway em Go expõe REST pro cliente, traduzindo internamente pra gRPC entre `bets-service` e `odds-service`. Deploy dos 4+ serviços em Kubernetes, com Ingress, ConfigMaps e HPA no `bets-service` (maior tráfego). Observabilidade completa (Prometheus + Grafana + Jaeger) em todos os serviços. Banco Postgres particionado por data de evento (o histórico de apostas cresce rápido).
Objetivo: integrar mensageria assíncrona (Kafka), comunicação síncrona (gRPC interno), deploy em Kubernetes com autoscaling seletivo e observabilidade distribuída num único fluxo de negócio real.
Critério de pronto: simular 100 jogos com atualização de placar via script, ver o fluxo completo do evento até a notificação, com trace distribuído visível no Jaeger.

### XXL 2 — Marketplace com Pedidos e Pagamento
O que construir: `catalog-service` mantém produtos (Postgres + Redis de cache); `orders-service` cria pedido e publica evento `pedido.criado` (RabbitMQ); `payment-service` consome o evento, simula um gateway de pagamento (aprovado/recusado com probabilidade configurável) e publica `pagamento.processado`; `inventory-service` consome o evento de pagamento aprovado e decrementa estoque com controle de concorrência. Implementa idempotência de verdade: reenviar o mesmo evento de pagamento duas vezes não pode duplicar o decremento de estoque (ex: registrando o ID do evento já processado). Deploy em ECS Fargate (não Kubernetes dessa vez, pra praticar AWS) + RDS + SQS no lugar do RabbitMQ. CloudWatch pra logs e alarmes de erro de pagamento.
Objetivo: praticar mensageria com garantias de idempotência em cenário de dinheiro real (onde duplicação é inaceitável), e a versão AWS-native do mesmo tipo de arquitetura do XXL 1.
Critério de pronto: simular 1000 pedidos concorrentes, garantindo que o estoque nunca fica negativo.

### XXL 3 — Sistema de Matchmaking de Jogos
O que construir: `queue-service` recebe a entrada do jogador na fila via gRPC streaming bidirecional (mantendo a conexão aberta); `matchmaking-service` roda em loop tentando formar partidas de 2-4 jogadores com critério de "elo" parecido; `session-service` cria a sessão da partida encontrada; `notification-service` avisa via streaming gRPC que a partida foi encontrada. Kubernetes com HPA no `matchmaking-service` baseado no tamanho da fila (métrica customizada via Prometheus Adapter — mais avançado que HPA por CPU). Tracing completo desde o momento que o jogador entra na fila até a partida começar.
Objetivo: praticar streaming bidirecional gRPC de ponta a ponta e autoscaling baseado em métrica de negócio (não infraestrutura), um caso mais avançado e menos comum que HPA por CPU.
Critério de pronto: simular 500 jogadores entrando na fila simultaneamente, medindo o tempo médio até formar partida.

### XXL 4 — Rastreador de Preços com Alertas
O que construir: `scraper-service` roda em cron (Kubernetes CronJob), busca preço de produtos configurados e publica `preco.atualizado` no Kafka; `history-service` consome e grava histórico de preço (Postgres particionado por produto+mês); `alert-service` consome, compara com o limite que o usuário configurou e decide se dispara alerta; `notification-service` envia o alerta (email fake via SMTP local, ou webhook). API REST pra o usuário cadastrar produtos e limites de preço. Observabilidade completa mais um dashboard Grafana mostrando quantos alertas disparam por dia.
Objetivo: integrar workload agendado (CronJob) com pipeline de eventos assíncrona e regra de negócio de alerta, num fluxo de ponta a ponta que precisa reagir rápido.
Critério de pronto: simular queda de preço artificial e ver o alerta chegar em menos de 1 minuto do evento.

### XXL 5 — Encurtador de URL com Analytics em Tempo Real
O que construir: `redirect-service` é o hot path — recebe `GET /:code`, resolve rapidamente usando Redis na frente do Postgres, redireciona o usuário, e publica o evento de clique no Kafka de forma assíncrona (sem bloquear o redirect). `analytics-service` consome os eventos de clique e agrega por país (via IP), dispositivo e hora do dia. `api-service` expõe REST pra criar links curtos e consultar analytics agregado. Deploy do `redirect-service` com HPA agressivo, já que é o caminho crítico e precisa escalar rápido sob pico. Faz load test com `k6` simulando 10 mil cliques em rajada, medindo se o redirect nunca fica lento mesmo com o Kafka publicando por trás.
Objetivo: praticar o princípio de "hot path nunca deve esperar por processamento assíncrono" — o clique é registrado sem nunca atrasar o redirect do usuário.
Critério de pronto: p95 de latência do redirect abaixo de 20ms mesmo sob carga, sem o analytics atrasar o redirect.

### XXL 6 — Plataforma de Reserva de Viagens (Saga)
O que construir: `flight-service`, `hotel-service`, `car-service` — cada um reserva seu recurso e publica evento de sucesso/falha (NATS JetStream). Implementa Saga coreografada: se `hotel-service` falhar, `flight-service` escuta o evento de falha e cancela a reserva de voo automaticamente (compensação). `booking-api` orquestra a experiência do usuário e consulta o status agregado da reserva. Implementa Outbox Pattern em cada serviço pra garantir que grava no banco e publica evento de forma atômica. Deploy em Kubernetes com Init Containers rodando migration antes de cada serviço subir. Tracing completo (Jaeger) mostrando a cadeia de compensação quando algo falha.
Objetivo: integrar o padrão Saga coreografada, Outbox Pattern e ordenação de inicialização (Init Containers) num único cenário de negócio realista e com múltiplas falhas parciais possíveis.
Critério de pronto: simular falha do `hotel-service` e ver a reserva de voo sendo cancelada automaticamente, com trace completo do evento de falha até a compensação.

### XXL 7 — Sistema de Chat com Presença
O que construir: `chat-service` usa WebSocket + gRPC streaming bidirecional pra mensagens em tempo real; `presence-service` rastreia quem está online usando Redis (chave por usuário com TTL). Múltiplas réplicas do `chat-service` sincronizadas via Redis Pub/Sub (mensagem enviada numa réplica chega em usuário conectado em outra réplica). Interceptor gRPC de autenticação validando token em toda conexão. Deploy em Kubernetes com Network Policy restringindo quem pode falar com o Redis. Observabilidade: RED method + profiling de memória (conexões WebSocket abertas consomem recurso, um caso real bom pra profiling).
Objetivo: resolver o problema clássico de WebSocket sem estado compartilhado não escalar horizontalmente, usando Redis Pub/Sub como solução, além de reforçar segurança de rede interna.
Critério de pronto: dois clientes conectados em réplicas diferentes trocando mensagem em tempo real, com falha simulada de uma réplica sem perda de mensagem.

### XXL 8 — Orquestrador de Pedidos Serverless
O que construir: pedido chega via API Gateway; Step Functions orquestra o fluxo: validar estoque (Lambda + DynamoDB) → processar pagamento (Lambda) → EventBridge dispara notificação. ElastiCache guarda o estoque "quente" pra validação rápida antes de consultar o DynamoDB. CloudWatch Alarms pra qualquer etapa do Step Functions que falhar.
Objetivo: praticar orquestração centralizada (Step Functions) como contraponto direto ao padrão Saga coreografada do XXL 6, comparando os dois modelos na prática dentro do mesmo tipo de fluxo de negócio.
Critério de pronto: rodar 100 pedidos simulados, um deles com pagamento recusado de propósito, e ver o Step Functions tratar o caminho de erro corretamente.

### XXL 9 — Motor de Busca Interno
O que construir: `indexer-service` recebe documentos (ex: artigos, produtos) e indexa usando o full-text search nativo do Postgres; `search-api` expõe a busca via REST, com cache Redis pros termos mais buscados; `analytics-service` consome eventos de busca (o que foi buscado, quantos resultados) via NATS. Réplica de leitura no Postgres pra separar a carga de indexação (escrita) da carga de busca (leitura). PgBouncer na frente pra suportar muitas conexões simultâneas de busca. Load test simulando 1000 buscas/segundo, medindo se a réplica de leitura aguenta sem afetar a indexação.
Objetivo: integrar full-text search, separação de carga leitura/escrita e connection pooling num serviço de busca real, sem depender de Elasticsearch.
Critério de pronto: p95 de latência de busca abaixo de 50ms mesmo com indexação rodando em paralelo.

### XXL 10 — Plataforma de Monitoramento de Infraestrutura
O que construir: `collector-service` coleta métricas de outros serviços via Prometheus scraping; `alert-service` avalia regras (RED method) e dispara alertas via Alertmanager; `dashboard-api` serve dados agregados pro Grafana. Deploy com StatefulSet pro Prometheus (precisa de armazenamento persistente e identidade estável). Kustomize com overlay `dev`/`prod` diferenciando retenção de métricas e réplicas. Service mesh (Linkerd) gerenciando mTLS entre os serviços internos, sem você codar isso manualmente.
Objetivo: construir uma plataforma que monitora outros sistemas usando exatamente as peças de Kubernetes/observabilidade estudadas na Parte 1, como projeto que "monitora a si mesmo".
Critério de pronto: derrubar um serviço monitorado de propósito e ver o alerta chegar automaticamente, mais o tráfego entre serviços internos criptografado via mTLS sem mudança de código.

### XXL 11 — Banco Digital com Event Sourcing
O que construir: `account-service` — toda movimentação (depósito, saque, transferência) é um evento gravado (Event Sourcing), nunca sobrescreve saldo direto. Mantém uma projeção materializada com saldo atual pra consulta rápida (CQRS). `auth-service` implementa JWT + RBAC (cliente comum vs. operador do banco com mais permissões). Circuit breaker entre `account-service` e um serviço externo simulado de verificação de fraude. Multi-tenancy por schema (cada "banco branco"/parceiro white-label isolado). Pipeline CI/CD completo rodando testes de integração com Testcontainers antes de qualquer deploy.
Objetivo: integrar Event Sourcing + CQRS num domínio onde histórico auditável é obrigatório por natureza (movimentação financeira), somado a autenticação/autorização e resiliência.
Critério de pronto: reconstruir o saldo de uma conta do zero só reaplicando os eventos, e provar que bate com a projeção materializada.

### XXL 12 — API Pública Multi-Cliente (SaaS B2B)
O que construir: API GraphQL (não REST) expondo dados de um catálogo de produtos por tenant. Rate limiting e cota por tenant (Redis), com plano free/pago tendo limites diferentes. Row-Level Security garantindo isolamento de dados entre tenants no Postgres. DataLoader resolvendo N+1 nas queries de produtos + categorias. Feature flags controlando quais tenants têm acesso a uma funcionalidade nova em beta. Testes de contrato (Pact) garantindo que o frontend (consumidor fictício) não quebra quando a API evolui.
Objetivo: integrar GraphQL avançado (DataLoader), multi-tenancy com isolamento real (RLS) e cota por cliente num cenário típico de produto SaaS B2B vendido por plano.
Critério de pronto: dois tenants diferentes usando a API simultaneamente sem nunca ver dado um do outro, e um tenant estourando cota recebendo erro 429 corretamente.

### XXL 13 — Plataforma de Assinatura com Resiliência
O que construir: `subscription-service` gerencia planos e cobrança recorrente. Chama um `payment-gateway-service` simulado que falha aleatoriamente de propósito — circuit breaker e retry com backoff protegendo o fluxo. Bulkhead limitando quantas chamadas simultâneas podem ir pro gateway de pagamento, isolado do resto do sistema. Chaos Engineering: script que mata o `payment-gateway-service` aleatoriamente durante teste de carga, validando que o resto do sistema continua respondendo. OAuth2 pra login de cliente via Google.
Objetivo: combinar as três técnicas de resiliência (circuit breaker, bulkhead, chaos engineering) num único fluxo de negócio recorrente e sensível a falha de terceiro (gateway de pagamento).
Critério de pronto: rodar teste de carga com o gateway de pagamento instável e nenhum outro serviço ficar indisponível por causa disso.

### XXL 14 — Sistema de Auditoria com Event Sourcing e GraphQL
O que construir: toda ação relevante do sistema (criação, edição, exclusão de qualquer entidade) vira um evento imutável gravado. API GraphQL expõe o histórico completo de qualquer entidade (quem mudou o quê e quando) com subscription notificando em tempo real quando uma nova mudança acontece. RBAC controlando quem pode ver o histórico de auditoria (só admin). Criptografia de campos sensíveis no histórico armazenado. Pipeline CI/CD com feature flag controlando o rollout gradual da funcionalidade de auditoria pra só alguns tenants primeiro.
Objetivo: aplicar Event Sourcing a um caso de auditoria (em vez de saldo financeiro como no XXL 11), somando GraphQL em tempo real e controle de acesso granular sobre dado sensível.
Critério de pronto: editar uma entidade e ver a mudança aparecer via subscription GraphQL em tempo real, com o evento correto gravado permanentemente.

### XXL 15 — Plataforma de Testes de Resiliência (meta: testa outros sistemas)
O que construir: `chaos-controller` orquestra experimentos de chaos engineering (mata pods, injeta latência, derruba conexão de rede) contra outros serviços do cluster. `resilience-dashboard` é uma API GraphQL mostrando o resultado dos experimentos (serviço se recuperou em quanto tempo, quantas requisições falharam durante o experimento). Autenticação RBAC: só operadores autorizados podem disparar um experimento de chaos em produção. Circuit breaker e bulkhead nos serviços alvo sendo validados experimentalmente pelos próprios experimentos. CI/CD que roda um experimento de chaos automaticamente em ambiente de staging antes de aprovar um deploy pra produção.
Objetivo: fechar a trilha de resiliência com uma plataforma que institucionaliza o chaos engineering como parte do próprio pipeline de deploy, não como um evento isolado manual.
Critério de pronto: disparar um experimento que mata metade das réplicas de um serviço alvo e o dashboard mostrar o tempo exato de recuperação via HPA/probes.

---

## PARTE 3 — Projetos GG (versão simplificada, cobrindo o essencial) — Trilha Go

**1. Lista de Tarefas Distribuída**
O que construir: API CRUD de tarefas (Postgres) + fila RabbitMQ simples que publica "tarefa criada" e um serviço separado só loga a mensagem. Deploy básico em Kubernetes (1 Deployment, 1 Service). Prometheus básico contando requisições.
Objetivo: sentir o ciclo completo (API → fila → consumer → deploy → métrica) sem nenhuma complexidade de negócio no meio do caminho.

**2. Blog com Cache**
O que construir: API de posts (Postgres) com Redis cacheando os posts mais lidos. Um único serviço, sem microsserviços. Deploy em ECS Fargate + RDS (prática de AWS isolada).
Objetivo: aprender cache invalidation e AWS básico sem se preocupar com múltiplos serviços conversando entre si.

**3. Chat Simples com WebSocket**
O que construir: um serviço Go com WebSocket, onde mensagens passam por um Redis Pub/Sub pra permitir múltiplas instâncias do serviço conversarem entre si. Deploy em Kubernetes com 2+ réplicas (prova que o Pub/Sub é necessário pra sincronizar).
Objetivo: entender por que WebSocket sem estado compartilhado não escala horizontalmente, antes de partir pro XXL 7 completo (com presença).

**4. Encurtador de URL (versão simples)**
O que construir: só `redirect-service` + Postgres, sem Kafka nem analytics em tempo real. Cache Redis na frente. Observabilidade básica (Prometheus só).
Objetivo: praticar cache e alta leitura sem a complexidade de mensageria — é a versão "sem o XXL 5".

**5. Monitor de Preço (versão simples)**
O que construir: um único serviço com cron interno (não CronJob do k8s) que checa preço e grava direto no Postgres. Sem Kafka: se o preço mudou, já dispara o alerta na mesma execução (síncrono). Deploy simples em Kubernetes, sem HPA.
Objetivo: entender o problema de negócio antes de adicionar mensageria — é a versão "sem o XXL 4".

**6. Reserva Simples com Compensação**
O que construir: só 2 serviços (`flight-service`, `hotel-service`) com Saga coreografada via NATS, sem Outbox Pattern nem tracing completo.
Objetivo: entender o conceito de compensação isoladamente, sem toda a complexidade do XXL 6.

**7. Chat Básico (sem presença)**
O que construir: um único `chat-service` com WebSocket, sem múltiplas réplicas nem Redis Pub/Sub.
Objetivo: aprender WebSocket puro antes de se preocupar em escalar horizontalmente (contraste direto com o GG 3 acima, que já resolve o problema de múltiplas réplicas).

**8. API Serverless Simples**
O que construir: uma Lambda + DynamoDB + API Gateway, sem Step Functions nem EventBridge.
Objetivo: primeiro contato com serverless, sem orquestração de múltiplas etapas.

**9. Busca com Postgres**
O que construir: `search-api` com full-text search do Postgres e cache Redis, sem separação de réplica nem PgBouncer.
Objetivo: aprender full-text search isoladamente, antes de somar réplica e pooling (XXL 9).

**10. Dashboard de Métricas Simples**
O que construir: um serviço só, instrumentado com RED method, Grafana mostrando o dashboard — sem Alertmanager, sem service mesh, sem StatefulSet.
Objetivo: dominar instrumentação básica antes de automatizar alerta.

**11. Conta Bancária Simples com Event Sourcing**
O que construir: só o `account-service` com eventos e projeção materializada, sem multi-tenancy nem circuit breaker.
Objetivo: entender Event Sourcing isoladamente, antes de somar auth, resiliência e multi-tenancy (XXL 11).

**12. API GraphQL com Rate Limit**
O que construir: uma API GraphQL simples com rate limiting por API key, sem multi-tenancy nem DataLoader.
Objetivo: primeiro contato com GraphQL + controle de uso, sem toda a complexidade do XXL 12.

**13. Assinatura com Circuit Breaker**
O que construir: `subscription-service` chamando um gateway de pagamento instável, só com circuit breaker (sem bulkhead nem chaos engineering).
Objetivo: aprender circuit breaker isoladamente antes de combinar com outras técnicas de resiliência.

**14. Log de Auditoria Simples**
O que construir: grava eventos de mudança numa tabela simples, expõe via REST (não GraphQL), sem subscription nem criptografia.
Objetivo: entender o conceito de auditoria imutável sem a complexidade de Event Sourcing completo.

**15. Pipeline CI/CD Básico**
O que construir: só o pipeline — lint → teste → build → deploy automático, aplicado a um projeto simples qualquer.
Objetivo: dominar automação de entrega antes de aplicar em projetos mais complexos.

---

---

# TRILHA TYPESCRIPT / NODE.JS (BACKEND)

Objetivo do dev nessa trilha: back-end. Diferente da trilha Go, aqui não tem frontend nem multiplataforma — é 100% servidor, banco, fila, cache e deploy, no que mais aparece em vaga real de Node/TS em 2026 (Express/Fastify, Prisma/Drizzle, JWT, Zod, Jest, Docker, filas com Redis/BullMQ, WebSocket). TypeScript desde o primeiro projeto, sem passar por JS solto — o JS "puro" fica reservado pro certificado do freeCodeCamp, que segue currículo próprio.

## PARTE 1 — Projetos por Tema (fundação)

### 🔹 FUNDAMENTOS TS/NODE (5 projetos)

**1. CLI de conversão de unidades / calculadora de IMC**
O que fazer: cria uma CLI simples em TS (roda com `ts-node` ou compilada com `tsc`) que converte unidades (km↔milhas, kg↔libras) ou calcula IMC a partir de argumentos de linha de comando. Define `interface`/`type` pros dados de entrada e saída, usa `enum` pra unidade escolhida, e trata entrada inválida lançando um erro tipado (classe de erro customizada, não `any`).
Objetivo: destravar a sintaxe básica de tipagem (interfaces, types, enums, union types) sem a complexidade de framework nenhum no meio.
Critério de pronto: rodar com entrada inválida (ex: unidade que não existe) e o programa recusar com mensagem clara, sem quebrar com stack trace cru.

**2. Parser de CSV tipado**
O que fazer: lê um arquivo CSV (ex: lista de produtos com nome/preço/quantidade), tipa cada linha com uma interface, valida linha por linha (preço não pode ser negativo, quantidade tem que ser inteiro) e gera um relatório (total em estoque, produto mais caro) ao final. Usa `fs/promises` pra leitura assíncrona.
Objetivo: praticar tipagem de dados vindos de fonte não confiável (arquivo externo) e tratamento de erro linha a linha sem abortar o processamento inteiro por causa de uma linha ruim.
Critério de pronto: um CSV com uma linha corrompida no meio não derruba o parser — ele reporta o erro daquela linha e continua processando as demais.

**3. Consumo de API pública tipado**
O que fazer: consome uma API pública (ex: clima, filmes, câmbio) com `fetch` nativo do Node, tipando a resposta com `interface`/`type` e usando um generic (`async function get<T>(url: string): Promise<T>`) pra reaproveitar a função de request em múltiplos endpoints. Trata erro de rede e resposta inesperada (status não-200, corpo fora do formato esperado) sem usar `any`.
Objetivo: entender como tipar dados que vêm de fora do seu controle (API de terceiro), incluindo o cenário onde a resposta real não bate com o tipo esperado.
Critério de pronto: simular a API fora do ar (URL errada) e o programa tratar o erro de forma tipada, sem `try/catch` genérico escondendo tudo atrás de `any`.

**4. Mini job scheduler com Node puro**
O que fazer: implementa um agendador simples usando só `setInterval`/`setTimeout` do Node (sem lib de fila ainda) que executa "tarefas" registradas (ex: função que loga hora, função que simula limpeza de cache) em intervalos configuráveis, com tipagem forte pra cada tarefa (`interface Task { name: string; intervalMs: number; run: () => void }`).
Objetivo: entender o event loop do Node na prática antes de esconder isso atrás de uma lib de fila (BullMQ) mais adiante na trilha.
Critério de pronto: registrar duas tarefas com intervalos diferentes e ver ambas rodando de forma independente sem bloquear uma a outra.

**5. CLI de leitura/escrita de JSON com validação manual**
O que fazer: lê um arquivo JSON (ex: lista de usuários), valida a estrutura manualmente (sem lib ainda — só `typeof`, checagem de campo obrigatório) antes de aceitar como um tipo válido, e escreve de volta no arquivo depois de uma modificação (ex: adicionar um usuário).
Objetivo: sentir na mão o problema que o Zod (tema de Validação, mais adiante) resolve — validar em runtime que um dado desconhecido realmente bate com o tipo que você espera.
Critério de pronto: um JSON com campo faltando ou tipo errado é rejeitado com mensagem específica de qual campo falhou, não um erro genérico.

---

### 🔹 API REST — EXPRESS/FASTIFY (5 projetos)

**1. CRUD básico em memória**
O que fazer: cria uma API Express (ou Fastify) em TS com CRUD completo de um recurso simples (ex: tarefas), guardando os dados num array em memória (sem banco ainda). Implementa os status codes corretos (201 na criação, 404 quando não encontra, 204 no delete) e um middleware de tratamento de erro centralizado.
Objetivo: fixar o básico de rota, middleware e ciclo request/response tipado (`Request`, `Response` do Express com tipos genéricos pro body) antes de somar banco de dados.
Critério de pronto: testar todos os verbos HTTP (GET/POST/PUT/DELETE) manualmente (Postman/Insomnia/curl) e cada um responder com o status code correto.

**2. API com paginação, filtro e ordenação**
O que fazer: evolui o CRUD anterior adicionando suporte a query params (`?page=2&limit=10&sort=nome&order=asc&status=pendente`), tipando os query params recebidos e validando valores fora do esperado (ex: `page=abc`) antes de processar.
Objetivo: entender como toda API de listagem real em produção lida com volume de dados sem devolver tudo de uma vez.
Critério de pronto: pedir uma página que não existe (ex: página 999 de uma lista pequena) retorna lista vazia, não erro.

**3. Versionamento de rota**
O que fazer: expõe a mesma funcionalidade em duas versões de contrato diferentes (`/v1/tarefas` retorna um formato, `/v2/tarefas` retorna um formato mudado, ex: campo renomeado ou estrutura aninhada diferente), ambas rodando ao mesmo tempo no mesmo processo.
Objetivo: entender como evoluir uma API sem quebrar cliente antigo que ainda depende do contrato anterior.
Critério de pronto: as duas versões respondem simultaneamente sem interferir uma na outra, mesmo compartilhando a mesma lógica de negócio por baixo.

**4. Upload de arquivo com validação**
O que fazer: implementa endpoint de upload (ex: foto de perfil) usando `multer`, validando tipo MIME (só aceita imagem) e tamanho máximo do arquivo antes de salvar em disco ou storage local.
Objetivo: praticar um tipo de endpoint que quase toda vaga real inclui (upload) e que tem particularidades (multipart/form-data) diferentes de JSON puro.
Critério de pronto: tentar subir um arquivo de tipo errado (ex: `.exe`) ou maior que o limite e a API rejeitar com mensagem clara, sem salvar nada em disco.

**5. Migração de Express pra Fastify**
O que fazer: pega a API do projeto 1 (ou 2) e reimplementa a mesma lógica em Fastify, usando o schema de validação nativo do Fastify (JSON Schema) em vez de validação manual.
Objetivo: sentir a diferença de performance e de validação nativa embutida no framework (Fastify valida e serializa por schema) versus fazer tudo manualmente no Express.
Critério de pronto: rodar um benchmark simples (`autocannon`) nas duas versões e comparar requisições por segundo.

---

### 🔹 BANCO DE DADOS (5 projetos)

**1. CRUD com Postgres + Prisma**
O que fazer: conecta a API REST a um Postgres real usando Prisma como ORM — define o `schema.prisma`, roda a primeira migration, e reescreve o CRUD do tema anterior usando o Prisma Client (totalmente tipado, sem SQL cru). Modela uma relação simples 1:N (ex: usuário tem várias tarefas).
Objetivo: aprender o ORM mais usado em vaga TS hoje, incluindo o fluxo de migration versionada (não é `ALTER TABLE` manual).
Critério de pronto: rodar `prisma studio` e ver os dados reais criados pela API aparecendo na interface do Prisma.

**2. Mesma API com Drizzle ORM**
O que fazer: reimplementa o CRUD do projeto anterior trocando Prisma por Drizzle, que gera SQL mais explícito (você vê a query real que ele monta) em vez de esconder tudo atrás de uma API totalmente abstrata.
Objetivo: comparar DX (developer experience) e nível de controle sobre o SQL gerado entre os dois ORMs mais citados em vaga TS — ajuda a defender de forma consciente qual escolher numa entrevista.
Critério de pronto: rodar a mesma query complexa (ex: filtro + join) nos dois ORMs e conseguir explicar a diferença do SQL gerado por cada um.

**3. Relação N:N com tabela pivot**
O que fazer: modela uma relação muitos-para-muitos real (ex: post com várias tags, tag pertencendo a vários posts) usando uma tabela pivot (`post_tags`), com o ORM escolhido. Implementa endpoint que retorna post já com as tags populadas (join), e endpoint que adiciona/remove uma tag de um post sem duplicar linha na pivot.
Objetivo: sair do CRUD simples 1:N e lidar com o tipo de relação que aparece toda hora em modelagem real (categorias, permissões, tags).
Critério de pronto: adicionar a mesma tag duas vezes no mesmo post não cria duas linhas na tabela pivot (idempotência da associação).

**4. Paginação por cursor**
O que fazer: numa tabela com volume razoável de dados simulados (ex: 100k+ linhas geradas por script), implementa paginação baseada em cursor (ex: `WHERE id > :ultimoId ORDER BY id LIMIT :n`) em vez de `OFFSET`, e compara a performance das duas abordagens conforme a página avança.
Objetivo: entender por que `OFFSET` fica lento em tabelas grandes (o banco ainda percorre todas as linhas puladas) e por que cursor não tem esse problema.
Critério de pronto: medir e documentar o tempo de resposta da página 1 vs. página 1000 nas duas abordagens — cursor deve se manter estável, `OFFSET` deve degradar visivelmente.

**5. Soft delete + auditoria**
O que fazer: em vez de `DELETE` de verdade, implementa soft delete (coluna `deleted_at`, registro só é ocultado das queries normais via middleware do ORM ou filtro padrão). Adiciona também uma tabela ou coluna de auditoria simples que registra quem alterou o quê e quando (ex: `updated_by`, `updated_at`, ou uma tabela `audit_log` separada).
Objetivo: entender por que sistemas reais raramente apagam dado de verdade — histórico e recuperação de erro humano dependem disso.
Critério de pronto: "deletar" um registro, confirmar que ele some das listagens normais mas ainda existe no banco, e conseguir restaurá-lo revertendo o soft delete.

---

### 🔹 AUTENTICAÇÃO E SEGURANÇA (5 projetos)

**1. JWT + bcrypt**
O que fazer: implementa registro (hash de senha com `bcrypt` antes de salvar) e login (compara senha e gera um JWT assinado com expiração). Cria um middleware que valida o token no header `Authorization` e protege rotas específicas, injetando o usuário autenticado no `Request` de forma tipada (extensão do tipo `Request` do Express).
Objetivo: entender o fluxo completo de autenticação que aparece em praticamente toda vaga backend, sem depender de lib pronta de auth completa (tipo Passport) na primeira vez.
Critério de pronto: chamar uma rota protegida sem token retorna 401; com token expirado ou adulterado também retorna 401; com token válido, retorna o recurso normalmente.

**2. Refresh token com rotação**
O que fazer: evolui o projeto anterior separando access token (curta duração, ex: 15 min) de refresh token (longa duração, ex: 7 dias, armazenado no banco ou Redis). Implementa endpoint `/refresh` que troca um refresh token válido por um novo par de tokens, invalidando o refresh token antigo (rotação — usá-lo de novo depois de trocado deve falhar).
Objetivo: entender por que token de vida curta sozinho é ruim pra experiência do usuário (login toda hora) e por que refresh token sem rotação é um risco de segurança se vazar.
Critério de pronto: usar o mesmo refresh token duas vezes seguidas — a segunda tentativa deve ser rejeitada.

**3. RBAC simples (Role-Based Access Control)**
O que fazer: adiciona papéis ao usuário (`admin`, `user`) e cria um middleware de autorização que verifica o papel antes de liberar certas rotas (ex: só `admin` pode deletar recurso de outro usuário). Testa os três cenários: sem token, com token de `user` tentando rota de admin, com token de `admin`.
Objetivo: entender a diferença entre autenticação ("quem é você") e autorização ("o que você pode fazer"), que é confundida com frequência por quem está começando.
Critério de pronto: usuário comum tentando acessar rota de admin recebe 403 (não 401 — ele está autenticado, só não autorizado).

**4. Rate limiting por IP ou API key**
O que fazer: implementa limite de requisições (ex: 100 req/min) usando `express-rate-limit` com Redis como store (pra funcionar mesmo com múltiplas instâncias da API rodando), diferenciando o limite por API key em vez de só por IP.
Objetivo: entender como proteger uma API pública de abuso, e por que o contador de rate limit não pode viver só em memória do processo se a API roda em mais de uma instância.
Critério de pronto: estourar o limite manualmente (script disparando requisições em loop) e receber 429, com o contador resetando corretamente depois da janela de tempo configurada.

**5. OAuth2 com Google ou GitHub**
O que fazer: implementa o fluxo Authorization Code do OAuth2 pra login social — redireciona o usuário pro provedor, recebe o callback com o código, troca por um token de acesso, busca o perfil do usuário no provedor e cria/atualiza o usuário local, emitindo seu próprio JWT ao final.
Objetivo: entender um fluxo de autenticação de terceiros, bem diferente de implementar auth própria — a maior parte da complexidade é o redirect e a troca de código por token, não a lógica de negócio.
Critério de pronto: um usuário novo faz login pela primeira vez via Google/GitHub e já aparece cadastrado no seu banco sem nunca ter preenchido um formulário de registro.

---

### 🔹 VALIDAÇÃO, ARQUITETURA E BOAS PRÁTICAS (5 projetos)

**1. Validação de entrada com Zod**
O que fazer: substitui a validação manual dos projetos anteriores por schemas Zod, usando `z.infer<typeof schema>` pra gerar o tipo TS automaticamente a partir do schema de validação (uma fonte única de verdade, em vez de manter `interface` e validação separadas e correndo o risco de ficarem dessincronizadas).
Objetivo: aprender a ferramenta que virou padrão de fato em validação TS — tipagem e validação de runtime vêm do mesmo lugar.
Critério de pronto: mandar um body com campo de tipo errado (ex: string onde espera número) e receber uma mensagem de erro específica de qual campo e por quê, gerada automaticamente pelo Zod.

**2. Camada de repositório abstraída por interface**
O que fazer: define uma interface `TarefaRepository` (`create`, `findById`, `findAll`, `update`, `delete`) e implementa duas versões: uma em memória e uma com Postgres/Prisma. A camada de serviço (regra de negócio) depende só da interface, nunca da implementação concreta.
Objetivo: entender por que separar regra de negócio do jeito que os dados são persistidos facilita trocar de banco (ou testar sem banco nenhum) sem tocar na lógica principal.
Critério de pronto: trocar a implementação injetada (memória → Postgres) sem mudar uma linha da camada de serviço.

**3. Clean Architecture básica (controller → service → repository)**
O que fazer: organiza um projeto de porte médio em camadas explícitas: controller (recebe HTTP, chama serviço), service (regra de negócio pura, sem saber que existe HTTP ou banco), repository (acesso a dado). Nenhuma camada de fora sabe detalhe de implementação da camada de dentro.
Objetivo: aprender a estrutura que aparece em praticamente toda vaga pleno como "arquitetura em camadas" ou "separação de responsabilidades" — sem precisar de framework de arquitetura pronto pra isso.
Critério de pronto: escrever um teste unitário do `service` sem precisar subir servidor HTTP nem banco de dados real.

**4. Tratamento de erro centralizado**
O que fazer: cria classes de erro customizadas (`NotFoundError`, `ValidationError`, `UnauthorizedError`) que carregam o status code HTTP correspondente, e um middleware de erro único no Express/Fastify que transforma qualquer erro lançado (conhecido ou não) numa resposta JSON padronizada, sem duplicar `try/catch` em cada rota.
Objetivo: entender como evitar que cada endpoint reimplemente sua própria lógica de tratamento de erro — um ponto único cuida disso pra toda a API.
Critério de pronto: lançar um erro não tratado explicitamente (ex: erro genérico de código) em qualquer rota e mesmo assim a API responder um JSON de erro padronizado, sem crashar o processo.

**5. Injeção de dependência manual**
O que fazer: numa API de porte médio (ex: 3-4 recursos com relação entre si), monta manualmente o grafo de dependências na inicialização (repository → service → controller, cada camada recebendo suas dependências via construtor), sem usar framework de DI pronto.
Objetivo: entender o conceito de inversão de controle antes de eventualmente usar um framework de DI de verdade (ex: InversifyJS, tsyringe) — saber o que a lib automatiza por baixo dos panos.
Critério de pronto: trocar uma implementação de dependência (ex: repositório de e-mail fake por um real) mudando só o ponto de montagem inicial, sem tocar nas classes que a usam.

---

### 🔹 TESTES E QUALIDADE (5 projetos)

**1. Testes unitários com Jest mockando repositório**
O que fazer: escreve testes unitários pra camada de serviço (regra de negócio) dos projetos anteriores, mockando a interface de repositório com `jest.mock` ou um mock manual — o teste roda sem tocar banco nenhum.
Objetivo: aprender a testar regra de negócio isolada, rápido, sem depender de infraestrutura externa.
Critério de pronto: rodar a suíte de testes inteira em menos de alguns segundos, sem nenhum banco ou serviço externo precisar estar no ar.

**2. Testes de integração com Testcontainers**
O que fazer: usa a lib Testcontainers pra subir um Postgres real dentro de um container só durante a execução dos testes (sem precisar de banco já rodando manualmente), roda as migrations reais e testa queries de verdade contra ele.
Objetivo: entender teste de integração confiável — mockar o banco esconde bug que só aparece com SQL de verdade (ex: constraint, tipo de coluna).
Critério de pronto: rodar a suíte de teste do zero numa máquina limpa (sem Postgres pré-instalado) e ela subir, rodar e derrubar o container sozinha.

**3. Testes de contrato de API com Supertest**
O que fazer: escreve testes que sobem a aplicação Express/Fastify e fazem requisição HTTP real contra ela (`supertest(app).post('/tarefas')...`), validando status code, formato do corpo de resposta e efeitos colaterais (ex: recurso realmente criado no banco de teste).
Objetivo: testar a API do jeito que o cliente real vai bater nela, cobrindo a integração entre rota, middleware e serviço junto.
Critério de pronto: um teste de contrato falha se alguém mudar o formato de resposta de um endpoint sem querer (ex: renomear um campo), funcionando como uma rede de segurança contra quebra de contrato.

**4. Coverage + lint automatizado no commit**
O que fazer: configura ESLint + Prettier no projeto, define uma meta mínima de cobertura de teste (ex: 70%) no Jest, e usa `husky` + `lint-staged` pra rodar lint e testes automaticamente antes de cada commit, bloqueando commit que viole as regras.
Objetivo: entender como times reais garantem qualidade mínima sem depender de disciplina manual de cada desenvolvedor lembrar de rodar lint/teste.
Critério de pronto: tentar commitar um código com erro de lint ou que quebra teste — o commit é bloqueado automaticamente antes de chegar no repositório.

**5. Mutation testing com Stryker**
O que fazer: roda o Stryker Mutator numa função crítica já testada (ex: cálculo de desconto, validação de saldo) — a ferramenta introduz pequenas mutações no código (ex: troca `>` por `>=`) e verifica se os testes existentes pegam a mudança. Se um teste não falha com a mutação, ele não estava testando o que deveria.
Objetivo: entender que 100% de cobertura de linha não significa teste bom — mutation testing mede se o teste realmente valida o comportamento, não só se executa a linha.
Critério de pronto: achar pelo menos uma mutação "sobrevivente" (não pega por nenhum teste) e escrever um teste novo especificamente pra matá-la.

---

### 🔹 MENSAGERIA E FILAS (5 projetos)

**1. Fila de jobs com BullMQ**
O que fazer: usa BullMQ (fila baseada em Redis) pra processar uma tarefa fora do ciclo de request/response (ex: enviar e-mail de boas-vindas depois de um cadastro) — a API só adiciona o job na fila e responde rápido pro cliente; um worker separado processa o job de fato.
Objetivo: entender por que tarefa lenta (envio de e-mail, geração de relatório) não deve travar a resposta HTTP, e como desacoplar isso com fila.
Critério de pronto: cadastrar um usuário e a resposta HTTP voltar imediatamente, enquanto o "envio de e-mail" acontece de forma visivelmente assíncrona no log do worker.

**2. Retry com backoff exponencial e Dead Letter Queue no BullMQ**
O que fazer: configura o job pra falhar de propósito nas primeiras tentativas (simulando erro), com retry automático usando backoff exponencial (BullMQ já suporta isso via configuração), e move o job pra uma fila de "falhos" depois de esgotar as tentativas.
Objetivo: entender o que acontece quando o processamento assíncrono falha de verdade — sem isso, job problemático fica reprocessando pra sempre ou se perde silenciosamente.
Critério de pronto: um job que sempre falha acaba na fila de falhos depois do número configurado de tentativas, sem travar o processamento dos outros jobs da fila principal.

**3. Pub/Sub simples com Redis**
O que fazer: implementa comunicação entre dois processos Node usando `PUBLISH`/`SUBSCRIBE` nativo do Redis (sem BullMQ) — um processo publica um evento (ex: "preço atualizado") e outro, rodando separado, reage a ele.
Objetivo: entender o modelo mais simples de pub/sub (sem persistência, sem garantia de entrega) antes de comparar com filas de verdade que garantem entrega.
Critério de pronto: desligar o assinante, publicar mensagens, ligar de novo — confirma que as mensagens publicadas nesse intervalo se perderam (característica do Pub/Sub simples do Redis).

**4. Outbox Pattern**
O que fazer: implementa um serviço que precisa gravar uma mudança no Postgres E publicar um evento correspondente de forma atômica — grava a mudança de negócio e um registro na tabela `outbox` na mesma transação, e um processo separado (poller) lê a `outbox` periodicamente e publica de fato (BullMQ, Redis Pub/Sub ou até um webhook simulado), marcando como publicado.
Objetivo: resolver o problema clássico do "dual write" — gravar em dois sistemas (banco + fila) sem coordenação pode causar perda ou duplicação de evento.
Critério de pronto: simular falha na publicação depois do commit do banco (ex: derruba o processo) e o poller ainda conseguir publicar o evento pendente na próxima execução, sem perder nem duplicar.

**5. Consumer de RabbitMQ em TS**
O que fazer: usando `amqplib`, implementa um consumer TS que escuta uma fila publicada por outro serviço (pode ser o producer em Go da trilha Go, se quiser integrar as duas trilhas de propósito) — declara a mesma fila/exchange, consome e processa a mensagem, com ack manual.
Objetivo: sentir mensageria interoperando entre linguagens diferentes — um evento publicado por um serviço Go sendo consumido por um serviço TS, cenário comum em empresa real com stack heterogênea.
Critério de pronto: publicar uma mensagem (de qualquer linguagem) e o consumer TS processar corretamente, com o mesmo comportamento de "não perder mensagem" testado nos projetos de mensageria da trilha Go.

---

### 🔹 CACHE E PERFORMANCE (5 projetos)

**1. Cache-aside com Redis**
O que fazer: numa query pesada (ex: listagem de produtos mais vendidos, calculada com agregação), implementa o padrão cache-aside — a API primeiro verifica se o resultado está no Redis; se não estiver, consulta o Postgres, guarda o resultado no Redis com um TTL, e retorna.
Objetivo: entender o padrão de cache mais comum em API real, incluindo a decisão de quanto tempo o dado pode ficar "levemente desatualizado" (TTL).
Critério de pronto: medir e comparar o tempo de resposta da primeira chamada (cache miss, vai no banco) contra a segunda chamada (cache hit, vem do Redis).

**2. Invalidação de cache ao atualizar dado**
O que fazer: evolui o projeto anterior — quando um produto é atualizado ou criado, a API invalida (remove) a chave de cache correspondente, forçando a próxima leitura a buscar o dado atualizado do banco em vez de servir um valor desatualizado até o TTL expirar.
Objetivo: entender o problema clássico de cache "stale" (dado velho servido pelo cache depois que a fonte de verdade mudou) e uma forma simples de resolver isso.
Critério de pronto: atualizar um produto e, na próxima leitura, ver o valor novo imediatamente — não precisa esperar o TTL expirar.

**3. Cota por usuário usando contador no Redis**
O que fazer: implementa um limite de uso diário por usuário (ex: 1000 requisições/dia numa rota específica) usando um contador no Redis com expiração configurada pra virar à meia-noite (ou 24h rolantes), bloqueando com 429 quando o limite estoura.
Objetivo: entender billing/cota aplicada de verdade — diferente de rate limit genérico por IP, aqui o controle é por identidade de cliente, cenário comum em API paga por plano.
Critério de pronto: estourar a cota de um usuário e confirmar que outro usuário continua funcionando normalmente (o limite é isolado por chave, não global).

**4. Profiling de uma API lenta com clinic.js**
O que fazer: propositalmente introduz um problema de performance numa rota (ex: loop ineficiente, alocação excessiva de array, função síncrona bloqueando o event loop) e usa `clinic.js` (ou `--prof` nativo do Node + `0x`) pra identificar exatamente onde está o gargalo.
Objetivo: aprender a ferramenta nativa/comum do ecossistema Node pra achar problema de performance real, em vez de só suspeitar de onde está o problema.
Critério de pronto: usar o relatório de profiling pra apontar a função exata responsável pelo gargalo, corrigir, e medir a melhora com um benchmark antes/depois.

**5. Benchmark documentado: índice + cache**
O que fazer: pega uma query lenta (sem índice, sem cache), mede o tempo de resposta; adiciona um índice apropriado no Postgres e mede de novo; depois adiciona cache Redis em cima e mede uma terceira vez. Documenta os três números lado a lado.
Objetivo: sair da teoria e enxergar numericamente o impacto de cada técnica isoladamente — importante pra saber justificar em entrevista por que escolheu uma abordagem em vez de outra.
Critério de pronto: ter uma tabela com os três tempos medidos (sem otimização / com índice / com índice + cache) documentada no README do projeto.

---

### 🔹 TEMPO REAL — WEBSOCKET/SSE (5 projetos)

**1. Chat simples com WebSocket**
O que fazer: implementa um chat básico usando `ws` (biblioteca mais crua) ou Socket.IO — dois ou mais clientes conectados trocam mensagem em tempo real através do servidor, sem precisar dar refresh na página ou fazer polling.
Objetivo: primeiro contato com conexão persistente bidirecional, diferente do modelo request/response do REST.
Critério de pronto: dois clientes conectados simultaneamente trocando mensagem e vendo a mensagem do outro aparecer sem refresh.

**2. Notificação em tempo real com SSE (Server-Sent Events)**
O que fazer: implementa um endpoint SSE que mantém a conexão HTTP aberta e envia eventos pro cliente sempre que um recurso muda no backend (ex: status de um pedido mudou), sem precisar de WebSocket completo (SSE é unidirecional server→cliente, mais simples pra esse caso de uso).
Objetivo: entender quando SSE resolve melhor que WebSocket — cenários onde só o servidor precisa "empurrar" atualização, sem o cliente precisar mandar mensagem de volta pela mesma conexão.
Critério de pronto: mudar o status de um pedido via outro endpoint REST e ver a notificação chegar automaticamente no cliente conectado via SSE, sem ele perguntar/dar polling.

**3. Presença online com Redis TTL**
O que fazer: cada usuário conectado via WebSocket grava uma chave no Redis com TTL curto (ex: 30s), renovada a cada heartbeat enviado pelo cliente; se o heartbeat parar (cliente caiu), a chave expira sozinha e o usuário passa a aparecer como offline.
Objetivo: entender uma forma simples e resiliente de rastrear "quem está online agora" sem depender de um evento explícito de desconexão (que pode nunca chegar se a conexão cair abruptamente).
Critério de pronto: matar a conexão do cliente de forma abrupta (fechar aba à força, sem evento de close) e o usuário aparecer como offline depois do TTL expirar, mesmo sem nenhum evento de desconexão ter sido recebido.

**4. Múltiplas instâncias sincronizadas via Redis Pub/Sub**
O que fazer: sobe duas instâncias do serviço de chat do projeto 1 rodando em processos/portas diferentes (simulando múltiplas réplicas). Sem sincronização, um cliente conectado na instância A não recebe mensagem de um cliente conectado na instância B. Resolve usando Redis Pub/Sub — toda mensagem publicada é também repassada pro Redis, e cada instância assina o canal pra propagar pros seus próprios clientes conectados.
Objetivo: entender por que WebSocket sem estado compartilhado não escala horizontalmente — é o mesmo problema resolvido na trilha Go, aqui replicado em TS.
Critério de pronto: cliente conectado na instância A e cliente conectado na instância B trocando mensagem em tempo real como se estivessem no mesmo servidor.

**5. Dashboard com métricas atualizando ao vivo via WebSocket**
O que fazer: um serviço processa eventos (ex: pedidos sendo criados, simulados por um script) e transmite uma métrica agregada atualizada (ex: contagem de pedidos na última hora) pra um dashboard conectado via WebSocket, sem o dashboard precisar dar refresh ou fazer polling.
Objetivo: aplicar tempo real a um caso de uso de visualização de dado agregado, não só chat — cenário comum em painel operacional.
Critério de pronto: disparar eventos simulados e ver o número no dashboard mudando sozinho, em tempo real, sem interação do usuário.

---

### 🔹 DOCKER, CI/CD E DEPLOY (5 projetos)

**1. Dockerfile multi-stage pra API TS**
O que fazer: escreve um Dockerfile com stage de build (instala dependências, compila TS pra JS) separado do stage final de runtime (só copia o JS compilado e as dependências de produção, numa imagem base menor como `node:alpine`), reduzindo bastante o tamanho da imagem final.
Objetivo: entender por que não faz sentido rodar `ts-node` ou carregar `devDependencies` inteiras em produção — o build deve acontecer antes, fora do container final.
Critério de pronto: comparar o tamanho da imagem final (multi-stage) com uma versão ingênua de single-stage que inclui tudo — a diferença deve ser visível.

**2. docker-compose com API + Postgres + Redis**
O que fazer: escreve um `docker-compose.yml` que sobe a API, o Postgres e o Redis juntos com um único comando (`docker-compose up`), incluindo variáveis de ambiente, volume persistente pro Postgres, e um `depends_on`/health check garantindo que a API só tenta conectar no banco depois dele estar pronto.
Objetivo: aprender o setup de desenvolvimento local mais comum em time real — ninguém instala Postgres/Redis manualmente na máquina, tudo sobe junto via compose.
Critério de pronto: um colega (ou você numa máquina limpa) clona o repositório e sobe o projeto inteiro rodando com um único comando, sem passo manual de instalação de banco.

**3. Pipeline GitHub Actions completo**
O que fazer: configura um workflow que roda automaticamente em cada push/PR: instala dependências, roda lint (ESLint), roda os testes (Jest), builda a imagem Docker, e (opcionalmente) faz push pra um registry, tudo em estágios separados que bloqueiam o próximo se o anterior falhar.
Objetivo: automatizar o caminho do código até uma imagem pronta pra deploy, sem depender de alguém lembrar de rodar lint/teste manualmente antes de subir código.
Critério de pronto: abrir um PR com um teste quebrado de propósito e ver o pipeline falhar e bloquear o merge automaticamente.

**4. Deploy real em Railway/Render/Fly.io**
O que fazer: faz o deploy de uma API completa (com banco gerenciado pela própria plataforma ou externo) numa dessas plataformas, configurando variáveis de ambiente de produção e confirmando que o serviço responde publicamente.
Objetivo: sair do "roda na minha máquina" e ter uma URL pública de verdade — importante pra portfólio, já que recrutador raramente clona repositório pra testar localmente.
Critério de pronto: acessar a API publicada de um dispositivo diferente do seu (ex: celular) e ela responder corretamente.

**5. Health check + graceful shutdown**
O que fazer: adiciona um endpoint `/health` que verifica se a API consegue de fato falar com o banco (não só "processo está de pé"), e implementa graceful shutdown — ao receber sinal de término (`SIGTERM`), a aplicação para de aceitar novas conexões, espera as requisições em andamento terminarem, e só então encerra o processo.
Objetivo: entender por que matar o processo abruptamente (sem graceful shutdown) pode cortar uma requisição no meio — comportamento importante pra qualquer ambiente que reinicia containers com frequência (deploy, autoscaling, Kubernetes).
Critério de pronto: disparar uma requisição lenta de propósito e mandar `SIGTERM` pro processo ao mesmo tempo — a requisição em andamento termina normalmente antes do processo encerrar.

---

## PARTE 2 — Projetos XXL (agregam quase tudo) — Trilha TypeScript/Node.js

### XXL 1 — Rede Social Simples (API completa)
O que construir: API de posts/comentários/curtidas com Postgres + Prisma, autenticação JWT com refresh token e RBAC (usuário comum vs. moderador), validação de entrada com Zod em todos os endpoints, cache Redis nos posts mais lidos com invalidação ao editar, testes unitários (Jest, mockando repositório) e testes de contrato (Supertest) cobrindo os fluxos principais, tudo containerizado com docker-compose e pipeline de CI/CD (lint → teste → build → deploy).
Objetivo: integrar praticamente todos os temas centrais da trilha (auth, banco, validação, cache, testes, deploy) num único domínio de negócio reconhecível, o tipo de projeto que um recrutador entende o escopo só de olhar o README.
Critério de pronto: um moderador conseguir remover post de outro usuário (RBAC funcionando), um usuário comum não conseguir a mesma ação (403), e a suíte de testes rodando inteira no pipeline antes de qualquer deploy.

### XXL 2 — Backend de E-commerce com Processamento Assíncrono
O que construir: `catalog` (Postgres + cache Redis nos produtos), `orders` (cria pedido, grava evento na tabela outbox), um poller que lê a outbox e publica na fila (BullMQ), um worker que consome o evento de pedido criado e simula processamento de pagamento (aprovado/recusado com probabilidade, retry com backoff se falhar), decrementando estoque só depois da aprovação — com controle de concorrência pra estoque nunca ficar negativo.
Objetivo: aplicar Outbox Pattern + fila + idempotência num fluxo de negócio onde inconsistência (perder ou duplicar um evento de pagamento) tem custo real.
Critério de pronto: simular 500 pedidos concorrentes pro mesmo produto com estoque limitado e o estoque nunca ficar negativo, mesmo com falhas simuladas de pagamento no meio do caminho.

### XXL 3 — Chat em Tempo Real Escalável
O que construir: `chat-service` com WebSocket (Socket.IO), autenticação JWT na conexão, presença online via Redis com TTL, múltiplas instâncias do serviço sincronizadas via Redis Pub/Sub (mensagem enviada numa instância chega em cliente conectado em outra), deploy via docker-compose com pelo menos 2 réplicas do serviço atrás de um proxy simples.
Objetivo: provar na prática que WebSocket sem estado compartilhado não escala — o mesmo problema resolvido na trilha Go, aqui inteiro em TS, incluindo autenticação na conexão (não só HTTP tradicional).
Critério de pronto: dois clientes conectados em réplicas diferentes trocando mensagem em tempo real, e um deles aparecendo como offline automaticamente segundos depois de a conexão cair abruptamente.

### XXL 4 — Sistema de Notificações Multi-canal
O que construir: `notification-api` recebe pedido de notificação (ex: "avisar usuário X sobre Y"), publica na fila BullMQ; workers separados processam por canal (e-mail simulado via SMTP local, notificação em tempo real via SSE pro usuário conectado), com retry + dead letter queue pra notificações que falham repetidamente, e rate limiting pra não notificar o mesmo usuário em excesso no mesmo período.
Objetivo: integrar fila, retry/DLQ, tempo real (SSE) e rate limiting num caso de uso onde "confiabilidade de entrega" e "não incomodar demais o usuário" são objetivos que competem entre si.
Critério de pronto: derrubar o worker de e-mail de propósito, ver as notificações pendentes acumularem na fila sem se perder, e ao subir o worker de novo elas serem processadas — as que falharem repetidamente demais devem cair na DLQ, não ficar tentando pra sempre.

### XXL 5 — API SaaS Multi-tenant com Cota por Cliente
O que construir: API organizada em Clean Architecture (controller/service/repository) servindo múltiplos tenants no mesmo banco (coluna `tenant_id` em toda tabela relevante), RBAC por tenant (admin do tenant vs. usuário comum do tenant), rate limiting e cota diária por tenant via Redis (planos com limites diferentes), validação Zod em toda entrada, pipeline CI/CD completo rodando testes de integração com Testcontainers antes de qualquer deploy.
Objetivo: simular o tipo de arquitetura de um produto SaaS B2B real, onde isolamento entre clientes e controle de uso por plano são requisitos de negócio, não só técnicos.
Critério de pronto: dois tenants diferentes usando a API ao mesmo tempo sem nunca ver dado um do outro, e um tenant estourando sua cota diária recebendo 429 enquanto outro tenant continua funcionando normalmente.

### XXL 6 — Dashboard Operacional em Tempo Real
O que construir: `collector` recebe eventos de negócio simulados (ex: pedidos, cliques, erros) e publica no Redis; `aggregator` consome os eventos, mantém métricas agregadas em memória/Redis (contagem por minuto, taxa de erro); `dashboard-api` expõe essas métricas via WebSocket pra atualização ao vivo num painel, além de um endpoint REST tradicional pra consulta pontual; tudo com health check e graceful shutdown, deployado via docker-compose.
Objetivo: aplicar tempo real a um caso de painel operacional (não chat), combinando processamento de evento assíncrono com uma camada de apresentação que atualiza sozinha.
Critério de pronto: disparar uma rajada de eventos simulados via script e ver o painel refletindo a métrica agregada mudando em tempo real, sem refresh manual.

---

## PARTE 3 — Projetos GG (versão simplificada, cobrindo o essencial) — Trilha TypeScript/Node.js

### GG 1 — CRUD com Auth Básica
O que construir: API CRUD de um recurso simples (ex: tarefas) com Postgres + Prisma, login com JWT protegendo as rotas de escrita, sem refresh token nem RBAC.
Objetivo: sentir o ciclo completo (rota → validação → banco → auth básica) antes de somar complexidade — é a versão "sem o XXL 1".

### GG 2 — Loja Simples com Fila
O que construir: `catalog` + `orders` com Postgres, publicando evento de pedido criado direto numa fila BullMQ (sem outbox pattern, sem controle de concorrência sofisticado de estoque).
Objetivo: entender o problema de processamento assíncrono antes de resolver a parte difícil de consistência (dual write) — é a versão "sem o XXL 2".

### GG 3 — Chat Básico (sem presença, sem múltiplas instâncias)
O que construir: um único `chat-service` com WebSocket (Socket.IO), sem Redis Pub/Sub nem rastreio de presença.
Objetivo: aprender WebSocket puro antes de se preocupar em escalar horizontalmente — é a versão "sem o XXL 3".

### GG 4 — Notificação por E-mail com Fila
O que construir: fila BullMQ simples processando envio de e-mail simulado, com retry básico, sem multi-canal (SSE) nem dead letter queue.
Objetivo: entender fila e retry isoladamente antes de somar múltiplos canais e rate limiting — é a versão "sem o XXL 4".

### GG 5 — API Multi-tenant Simples
O que construir: API com coluna `tenant_id` em toda tabela e filtro manual por tenant em toda query, sem rate limiting nem cota, sem Clean Architecture formal.
Objetivo: entender o conceito de isolamento de dado por tenant antes de somar cota e arquitetura em camadas — é a versão "sem o XXL 5".

### GG 6 — Dashboard de Contagem Simples
O que construir: um serviço só que conta eventos simulados e expõe um número via REST comum (com polling simples no cliente), sem WebSocket nem agregação em tempo real.
Objetivo: entender o dado que quer mostrar antes de resolver o problema de atualização ao vivo — é a versão "sem o XXL 6".

## Como usar este roteiro mestre

### Lógica geral: um projeto por semana, intercalando trilha
A ideia central desse roteiro é ritmo semanal, alternando de linguagem/trilha a cada projeto pra treinar os dois ecossistemas em paralelo sem cansar do mesmo stack — por exemplo: semana 1 um projeto da Trilha Go, semana 2 um projeto da Trilha TypeScript/Node.js, semana 3 volta pra Go, e assim por diante. Dentro de cada trilha, a ordem interna é sempre tema isolado → GG → XXL (nunca pula direto pro XXL sem ter feito o tema isolado antes, senão a complexidade acumulada atropela o aprendizado). Nada impede fazer duas semanas seguidas da mesma trilha se um tema específico estiver rendendo muito — a alternância é recomendação, não regra rígida.

### Trilha Go
1. **Sub-trilhas independentes:** dentro da Trilha Go existem cinco frentes que também podem ser intercaladas entre si: (a) arquitetura de produção (Mensageria → Chaos Engineering, seguindo tema → GG → XXL), (b) Build Your Own X (fundamentos de sistemas, sem infra), (c) Multiplataforma (React/Next.js + Go + Tauri + Capacitor), (d) Banco de Dados + AWS por nível e (e) LLM/Agentes de IA.
2. **Prioridade na arquitetura de produção:** Mensageria, Kubernetes, AWS, Observabilidade e Banco de Dados primeiro (aparecem mais em vaga real); Auth/Segurança, Testes/CI-CD, CQRS/Event Sourcing, GraphQL, Multi-tenancy e Resiliência depois (diferenciais de sênior).
3. **Prioridade em Build Your Own X:** Redis, Database, Container e Load Balancer primeiro (mais cobrados em entrevista de sistemas); BitTorrent/Lexer/Regex depois; blockchain/redes neurais/CLIs por último ou como descompressão de fim de semana.
4. **Escolha na Multiplataforma:** se o objetivo é destacar backend/Go, priorize Ponto Certo, Radar de Preços ou Sala de Espera (regras de negócio, concorrência, tempo real). Se quer mostrar domínio de arquitetura de cliente complexa, priorize Vault Local ou Nota Rápida (criptografia, diff/merge). Se quer algo com mais "wow" visual/nativo, priorize Fluxo (widgets nativos são raros em portfólio).
5. **Banco de Dados + AWS:** siga a progressão de nível (1 → 2 → 3) e sempre aplique a dica de "quebrar de propósito" ao final de cada projeto — simular volume alto, derrubar e restaurar backup, calcular custo real.
6. **LLM/Agentes:** siga a ordem sugerida 1 → 2 → 6 → 3 → 4 → 5 — começa com integração de agente (rápido, motivador), depois RAG (fundamental), depois embeddings próprios, aí parte pro fine-tuning de LLM de verdade, e fecha com orquestração multi-agente e observabilidade.

### Trilha TypeScript/Node.js
7. **Prioridade de temas:** API REST, Banco de Dados, Auth/Segurança e Validação/Arquitetura primeiro (são a base que toda vaga backend TS/Node pede); Testes/Qualidade e Docker/CI-CD logo em seguida (diferencial que já pesa até em vaga júnior hoje); Mensageria/Filas, Cache/Performance e Tempo Real por último (mais avançado, mas ainda muito presente em vaga pleno).
8. **TypeScript desde o primeiro projeto:** nada de passar por JS puro nos projetos de portfólio — o JS solto fica reservado só pro currículo do freeCodeCamp (que segue trilha própria em JS pro certificado). Exercism e LeetCode podem ser resolvidos direto em TS.
9. **Papel de cada ferramenta de estudo paralela:** freeCodeCamp cobre fundamentos e serve pro certificado (não precisa forçar TS ali); Exercism afia sintaxe isolada (tem trilha própria de TS); LeetCode é pra lógica de entrevista técnica, não substitui projeto de portfólio.
10. Sem foco em frontend nessa trilha (objetivo é back) — os projetos fullstack (XXL 1 e XXL 2, por exemplo) usam só o mínimo de front necessário pra provar integração, nunca React/framework chique.

### Comum às duas trilhas
11. Documente cada projeto num README: o que foi feito, por quê, e o que quebrou no caminho — é isso que vira história de entrevista, em qualquer uma das duas trilhas.
12. Rastreio de origem — Trilha Go: XXL 1-5 e GG 1-5 vêm da lista original 1; XXL 6-10 e GG 6-10 da lista 2; XXL 11-15 e GG 11-15 da lista 3. As seções Multiplataforma, Banco de Dados+AWS e LLM/Agentes vieram de conversas separadas, sem XXL/GG próprios ainda — podem ganhar isso depois se fizer sentido. Trilha TypeScript/Node.js: os 50 projetos por tema, os 6 XXL e os 6 GG foram levantados com base em pesquisa direta de requisito de vaga real de backend Node/TS em 2026 (Express/Fastify, Prisma/Drizzle, JWT, Zod, Jest, Docker, BullMQ/Redis, WebSocket).
