from confluent_kafka import Producer


def delivery_report(err, msg):
    """Callback chamado quando a mensagem é entregue ou ocorre um erro."""
    if err is not None:
        print(f'Erro ao entregar a mensagem: {err}')
    else:
        print(f'Mensagem entregue: {msg.value()}')


def kafka_producer(bootstrap_servers, topic):
    # Configurações do produtor
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }

    # Cria o produtor
    producer = Producer(config)

    # Loop para enviar mensagens
    while True:
        message = input(
            "Digite uma mensagem para enviar (ou 'sair' para encerrar): ")
        if message == 'sair':
            break

        # Envia a mensagem para o tópico especificado
        producer.produce(topic, value=message, callback=delivery_report)

        # Espera a confirmação de entrega da mensagem
        producer.poll(0)

    # Aguarda a entrega de todas as mensagens pendentes antes de encerrar
    producer.flush()


if __name__ == '__main__':
    # Lista de servidores Kafka (separados por vírgula)
    bootstrap_servers = '192.168.109.130:9092'
    topic = 'ECOMMERCE_NEW_ORDER'  # Tópico para enviar as mensagens

    kafka_producer(bootstrap_servers, topic)
