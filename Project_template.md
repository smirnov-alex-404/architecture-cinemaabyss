
# Задание 1

TO-BE архитектура КиноБездны.
Система разделена на отдельные домены и организована единая точка вызова сервисов.

[Container diagram](./diagrams/Container.puml)

![Container diagram_img](./diagrams/Container.png)

# Задание 2

### 1. Proxy

[postman_tests](./img/postman.jpg)

![postman_tests](./img/postman.jpg)
 
[newman_tests](./img/newman.jpg)

![newman_tests](./img/newman.jpg)

### 2. Kafka

[kafka_img](./img/kafka.jpg)

![kafka_img](./img/kafka.jpg)

# Задание 3


### CI/CD

Доработан деплой новых сервисов proxy и events.
Успешным результатом данного шага является "зеленая" сборка и "зеленые" тесты.

![CI/CD](./img/cicd.jpg)
![CI/CD](./img/cicd-2.jpg)

### Proxy в Kubernetes

#### All services started via kuber

![Kuber](img/kuber.jpg)


#### Cкриншот вывода при вызове https://cinemaabyss.example.com/api/movies 

![Kuber_movies](img/kuber_movies.jpg)

#### Cкриншот вывода event-service после вызова тестов.

![Kuber_events](img/kuber_events.jpg)

#### Успешный запуск тестов через Newman

![Kuber_tests](img/kuber_tests_newman.jpg)

# Задание 4

Cкриншот развертывания helm и вывода https://cinemaabyss.example.com/api/movies

![Helm_deploy](img/helm_deploy.jpg)
![Helm_movies](img/helm_movies.jpg)