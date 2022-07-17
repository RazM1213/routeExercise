from configurations.RabbitMqPublisherConfigure import RabbitMqPublisherConfigure
import pika


class RabbitMqPublisher:
    def __init__(self, server: RabbitMqPublisherConfigure):
        self.server = server
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def publish(self, payload):
        self._channel.basic_publish(
            exchange=self.server.exchange,
            routing_key=self.server.routingKey,
            body=str(body)
        )

        print("Published Message:\n {}".format(payload))
        self._connection.close()


body = """{
  "studentDetails": {
    "firstName": "Raz",
    "lastName": "Matzliah",
    "id": 322717570
  },
  "subjectGrades": [
    {
      "subject": "Math",
      "grades": [
        100,
        90,
        96
      ]
    },
    {
      "subject": "English",
      "grades": [
        98,
        95,
        94
      ]
    },
    {
      "subject": "History",
      "grades": [
        100,
        97,
        85
      ]
    },
    {
      "subject": "Chemistry",
      "grades": [
        93,
        90,
        100
      ]
    }
  ],
  "birthDate": "27/06/2000",
  "age": 22,
  "gender": "זכר",
  "behaviourGrade": 8
}
"""


if __name__ == "__main__":
    server_configure = RabbitMqPublisherConfigure(
        queue='student_data',
        host='localhost',
        routingKey='student.data',
        exchange='student'
    )

    rabbitmq = RabbitMqPublisher(server_configure)
    rabbitmq.publish(body)
