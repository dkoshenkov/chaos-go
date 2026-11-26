# Kafka через CONNECT

Из корня репозитория запустите сквозную проверку с Docker и Docker Compose.

```sh
tests/e2e/kafka/smoke.sh
```

Сценарий собирает образ из `Dockerfile`, запускает Apache Kafka 4.1.2 в отдельном Compose-проекте, проверяет адрес брокера в метаданных, создаёт топик, публикует сообщение и проверяет его содержимое после чтения. Счётчик `chaos_proxy_injections_total{direction="request",fault_type="delay"}` подтверждает применение задержки. Второй адаптер запрашивает сброс соединения заголовком `X-Chaos-Connection-Reset: true`. Сценарий проверяет, что клиент через этот адаптер теряет соединение при запросе метаданных, а счётчик `chaos_proxy_tcp_connections_total{outcome="reset"}` растёт. После проверки он удаляет контейнеры, сети и тома проекта. Для запуска с уже собранным локальным образом задайте `CHAOS_PROXY_IMAGE`.

```sh
CHAOS_PROXY_IMAGE=chaos-proxy:dev tests/e2e/kafka/smoke.sh
```

Клиент Kafka работает в сети Compose и подключается к `adapter:19092`. Для каждого соединения `chaos-tunnel` посылает запрос CONNECT к `proxy:18080` с заголовком `X-Chaos-Request-Delay-Ms: 10` и токеном. Прокси открывает фиксированный upstream `broker:9092`, а после установки туннеля по нему идут байты Kafka. Брокер публикует `adapter:19092` для `CLIENT` listener через `advertised.listeners`, поэтому клиенты продолжают использовать туннель после получения метаданных. Межброкерный listener `broker:29092` и контроллер `broker:29093` доступны в сети брокера.

Для ручной проверки запустите стенд и выполните команды Kafka из контейнера `client`.

```sh
docker build -t chaos-proxy:kafka-smoke .
docker compose -f tests/e2e/kafka/compose.yaml -p chaos-kafka-demo up -d --wait
docker compose -f tests/e2e/kafka/compose.yaml -p chaos-kafka-demo run --rm client \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server adapter:19092 --list
docker compose -f tests/e2e/kafka/compose.yaml -p chaos-kafka-demo down --volumes
```
