package main

import (
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}
	// Leo la variable de entorno INPUT_FILE que me dice la ruta del archivo exacto que tiene que leer el cliente
	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	// Leo la variable de entorno OUTPUT_FILE que me dice la ruta del archivo exacto que tiene que leer el cliente
	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}
	// Convierto en int la variable de entorno BATCH_SIZE
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is not an integer")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- client.Run()
	}()
	
	select {
	// En caso de recibir un sigterm o alguna interrupcion
	// llamo el stop para cerrar la conexion y salir del programa
	case <-sigs:
		client.Stop()
		// Espero a que termine el run
		<-errChan
		logger.Info("client-shutdown", logger.Success)
		return 0
	// Si no fue una interrupcion y recibi un error
	case err := <-errChan:
		if err != nil {
			logger.Error("client-run", logger.Fail, "err", err)
			return 1
		}
		return 0
	}
}

func main() {
	os.Exit(run())
}
