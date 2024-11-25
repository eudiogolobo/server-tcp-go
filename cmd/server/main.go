package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants"
	"servidor-tcp-go/config"
	"servidor-tcp-go/pkg/database"

    "github.com/joho/godotenv"

)

type CommandData struct {
	DeviceID    int64  `json:"device_id"`
	Message     string `json:"message"`
	MessageType string `json:"message_type"`
}

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(addr, password string, db int) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		PoolSize: 50,  // Definir o tamanho do pool de conexões Redis
	})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Erro ao conectar no Redis: %v", err)
	}
	fmt.Println("Conexão com o Redis estabelecida com sucesso!")
	return &RedisQueue{client: client}
}

func (r *RedisQueue) Push(queueName string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("erro ao serializar dados: %v", err)
	}
	return r.client.LPush(context.Background(), queueName, jsonData).Err()
}

func (r *RedisQueue) Pop(queueName string) (string, error) {
	result, err := r.client.BRPop(context.Background(), 0, queueName).Result()
	if err != nil {
		return "", err
	}
	return result[1], nil
}

var (
	rdb                 *RedisQueue
	deviceChannels      = make(map[int64]chan string)
	channelsMutex       sync.RWMutex
	activeConnections   int32
	workerPool          *ants.Pool
)

func main() {
	var err error

    // Carregar as variáveis de ambiente do arquivo .env
	err = godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Erro ao carregar arquivo .env: %v", err)
	}

    // Acessando variáveis de ambiente
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		// Valor padrão caso a variável de ambiente não esteja definida
		redisHost = "127.0.0.1"
	}

	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}

    serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		serverPort = "8082"
	}

	// Inicializa o Redis
	rdb = NewRedisQueue(redisHost+":"+redisPort, "", 0)

	// Inicializa o pool de workers (Tamanho do pool ajustável)
	workerPool, err = ants.NewPool(100) // Número de workers no pool
	if err != nil {
		log.Fatalf("Erro ao criar pool de workers: %v", err)
	}
	defer workerPool.Release()

	// Conecta ao banco de dados
	db, err := config.ConnectToDatabase()
	if err != nil {
		log.Fatalf("Erro ao conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	// Captura sinais para desligamento
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	// Inicia o servidor TCP e o listener de comandos
	go startTCPServer(serverPort, db)
	go receiveCommands()

	fmt.Println("Servidor TCP iniciado. Pressione Ctrl+C para parar.")

	<-shutdownSignal
	fmt.Println("Recebido sinal de desligamento. Limpando recursos...")
	fmt.Println("Desligamento completo. Servidor encerrado.")
}

func startTCPServer(serverPort string, db *sql.DB) {

	listener, err := net.Listen("tcp", ":"+serverPort)
	if err != nil {
		log.Fatalf("Erro ao iniciar o servidor TCP: %v", err)
	}
	defer listener.Close()

	fmt.Println("Servidor TCP ouvindo na porta "+serverPort+"...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Erro ao aceitar conexão: %v", err)
			continue
		}

		atomic.AddInt32(&activeConnections, 1)
		fmt.Printf("Conexões ativas: %d\n", atomic.LoadInt32(&activeConnections))

		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *sql.DB) {
	defer func() {
		atomic.AddInt32(&activeConnections, -1)
		fmt.Printf("Conexões ativas: %d\n", atomic.LoadInt32(&activeConnections))
		conn.Close()
	}()

	// Defina o tempo limite de leitura inicial
	conn.SetReadDeadline(time.Now().Add(3 * time.Minute)) // Tempo limite para 3 minutos de inatividade

	reader := bufio.NewReaderSize(conn, 4096)  // Aumenta o buffer para melhorar a performance

    buffer := make([]byte, 4096)

	// Processa a mensagem inicial
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("Erro ao ler a mensagem: %v", err)
		return
	}

	message := strings.TrimSpace(string(buffer[:n]))

    log.Printf("Mensagem recebida: %s", message)

	// Valida formato da mensagem
	parts := strings.Split(message, ";")
	if len(parts) < 2 {
		log.Printf("Mensagem inválida recebida: %s", message)
		return
	}

	// Busca ID do dispositivo no banco
	deviceID := parts[1]
	id, err := database.GetDeviceID(db, deviceID)
	if err != nil {
		log.Printf("Erro ao verificar dispositivo: %v", err)
		return
	}
	if id == 0 {
		log.Printf("Dispositivo com ID %s não encontrado.", deviceID)
		return
	}

	log.Printf("Conexão estabelecida para dispositivo %d", id)

	// Garante canal do dispositivo
	channelsMutex.Lock()
	if _, exists := deviceChannels[id]; !exists {
		deviceChannels[id] = make(chan string)
	}
	ch := deviceChannels[id]
	channelsMutex.Unlock()

	// Goroutine para enviar comandos
	quit := make(chan bool)
	go sendCommandsToClient(conn, id, ch, quit)

	// Processa mensagens recebidas
	for {
		// A cada leitura de mensagem, se o tempo de inatividade for excedido, a conexão será fechada
		conn.SetReadDeadline(time.Now().Add(180 * time.Minute)) // Ajuste o tempo limite a cada mensagem recebida

		// Lê dados diretamente do buffer
		n, err := conn.Read(buffer)

		if err != nil {
			log.Printf("Erro ao ler mensagem: %v", err)
			close(quit)
			cleanupDeviceChannel(id)
			return
		}

		message := strings.TrimSpace(string(buffer[:n]))
        
		log.Printf("Mensagem recebida: %s", message)

		switch typeMessage(message) {
		case 0:
			enqueueRedisMessage(id, message, "satnew_database_status_queue", "SST")
		case 6:
			enqueueRedisMessage(id, message, "satnew_database_response_command_queue", "RES")
		default:
			log.Printf("Tipo de mensagem inválida: %s", message)
		}
	}
}

func sendCommandsToClient(conn net.Conn, id int64, ch chan string, quit chan bool) {
	for {
		select {
		case cmd := <-ch:
			_, err := fmt.Fprintf(conn, "%s\n", cmd)
			if err != nil {
				log.Printf("Erro ao enviar comando para o dispositivo %d: %v", id, err)
				return
			}
			log.Printf("Comando enviado para o dispositivo %d: %s", id, cmd)
		case <-quit:
			log.Printf("Encerrando goroutine para dispositivo %d", id)
			return
		}
	}
}

func cleanupDeviceChannel(id int64) {
	channelsMutex.Lock()
	defer channelsMutex.Unlock()
	delete(deviceChannels, id)
}

func receiveCommands() {
	for {
		message, err := rdb.Pop("satnew_database_send_command_queue")
		if err != nil {
			log.Printf("Erro ao consumir da fila Redis: %v", err)
			continue
		}

		log.Printf("Comando recebido da fila: %s", message)

		var commandData map[string]string
		err = json.Unmarshal([]byte(message), &commandData)
		if err != nil {
			log.Printf("Erro ao desserializar comando: %v", err)
			continue
		}

		deviceID, _ := strconv.ParseInt(commandData["id"], 10, 64)
		command := commandData["command"]

		channelsMutex.RLock()
		ch, exists := deviceChannels[deviceID]
		channelsMutex.RUnlock()

		if exists {
			// Enfileira o comando para o dispositivo
			err := workerPool.Submit(func() {
				ch <- command
			})
			if err != nil {
				log.Printf("Erro ao adicionar comando ao worker pool: %v", err)
			}
		} else {
            enqueueRedisMessage(deviceID, "Dispositivo não encontrado, comando não enviado.", "satnew_database_response_command_queue", "RES")
			log.Printf("Dispositivo %d não encontrado, comando não enviado.", deviceID)
		}
	}
}

func enqueueRedisMessage(deviceID int64, message, queueName, msgType string) {
	commandData := CommandData{
		DeviceID:    deviceID,
		Message:     message,
		MessageType: msgType,
	}
	err := rdb.Push(queueName, commandData)
	if err != nil {
		log.Printf("Erro ao adicionar mensagem à fila Redis: %v", err)
	}
}

func typeMessage(message string) int {
	arrayTypes := [][]string{
		{"ST300STT", "ST360STT"},
		{"ST300EMG"},
		{"ST300EVT"},
		{"ST300ALT"},
		{"ST300HTE"},
		{"ST300ALV"},
		{"ST300CMD"},
	}

	content := strings.Split(message, ";")
	if len(content) == 0 {
		log.Println("Mensagem inválida")
		return -1
	}

	for i, array := range arrayTypes {
		for _, device := range array {
			if device == content[0] {
				return i
			}
		}
	}

	log.Printf("Mensagem não salva: %s", message)
	return -1
}
