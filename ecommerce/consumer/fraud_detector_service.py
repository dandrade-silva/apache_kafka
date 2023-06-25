from confluent_kafka import Consumer, KafkaException, KafkaError


def kafka_consumer(bootstrap_servers, group_id, topic):
    # Configurações do consumidor
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        # Inicia a partir do início do tópico em caso de novo grupo
        'auto.offset.reset': 'earliest'
    }

    # Cria o consumidor
    consumer = Consumer(config)

    # Assina o tópico especificado
    consumer.subscribe([topic])

    try:
        while True:
            # Aguarda a chegada de mensagens
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fim do consumo do tópico
                    print(f'Fim do tópico alcançado: {msg.topic()}')
                else:
                    # Tratamento de erro
                    raise KafkaException(msg.error())
            else:
                # Processa a mensagem recebida
                print(f'Mensagem recebida: {msg.value().decode("utf-8")}')

    except KeyboardInterrupt:
        # Encerra o consumidor com interrupção do teclado
        pass

    finally:
        # Fecha o consumidor
        consumer.close()


if __name__ == '__main__':
    # Lista de servidores Kafka (separados por vírgula)
    bootstrap_servers = '192.168.109.130:9092'
    group_id = 'FRAUD_DETECTOR_SERVICE'  # ID do grupo do consumidor
    topic = 'ECOMMERCE_NEW_ORDER'  # Tópico para consumir as mensagens

    kafka_consumer(bootstrap_servers, group_id, topic)
