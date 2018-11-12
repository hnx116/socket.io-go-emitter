package main

import (
	"fmt"

	"github.com/hnx116/socket.io-go-emitter/emitter"
)

func main() {
	sio := emitter.New(emitter.Options{
		Host: "127.0.0.1:6379",
		Key:  "socket.io",
	})

	fmt.Println("Emit :", sio.Emit("broadcast event", "Hello from socket.io-go-emitter"))
	fmt.Println("In Emit :", sio.In("test").Emit("broadcast event", "Hello from socket.io-go-emitter"))
	fmt.Println("To Emit : ", sio.To("test").Emit("broadcast event", "Hello from socket.io-go-emitter"))
	fmt.Println("Of Emit :", sio.Of("/nsp").Emit("broadcast event", "Hello from socket.io-go-emitter"))
}
