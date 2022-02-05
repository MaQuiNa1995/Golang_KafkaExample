package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "Maqui-Topic1"
	brokerAddress = "localhost:29092"
	group         = "Maqui-Group"
)

func main() {

	// Creamos un contexto infinito
	ctx := context.Background()

	// Si quieres usar el productor y el consumidor en la misma ejecución deberás
	// ejecutar el productor en una rutina ya que ambos son bloqueantes
	go produce(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {
	// crea un nuevo logger que imprime en stdout
	l := log.New(os.Stdout, "Productor Kafka: ", 0)

	// crea un nuevo productor con los brokers y el topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// asigna el logger al productor
		Logger: l,
	})

	// creamos el mensaje que vamos a enviar
	err := w.WriteMessages(ctx, kafka.Message{
		// key del mensaje
		Key: []byte("key"),
		// valor del mensaje
		Value: []byte("Mensaje Golang"),
	})
	if err != nil {
		panic(err.Error())
	}

	// traza de log para verificar que se ha escrito el mensaje
	fmt.Println("Se ha producido el mensaje en kafka")

	// parada de 1 segundo en la ejecución
	time.Sleep(time.Second)
}

func consume(ctx context.Context) {

	// crea un logger que imprime en pantalla
	l := log.New(os.Stdout, "Consumidor kafka: ", 0)

	// inicializa un consumidor con los brokers y el topic
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// asignamos el logger
		Logger: l,
	})
	for {
		// bloqueamos la ejecución mientras esperamos nuevos mensajes
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic(err.Error())
		}
		// cuando los mensajes llegan los imprimimos
		fmt.Println("Recibido: ", string(msg.Value))
	}
}
