package utils

import (
	"log"
	"os"
)

func SetupLogging() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// If you want to add file logging later, you can uncomment and modify:
	// logFile, err := os.OpenFile("payment-service.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	// if err != nil {
	// 	log.Printf("Failed to open log file: %v", err)
	// 	return
	// }
	//
	// multiWriter := io.MultiWriter(os.Stdout, logFile)
	// log.SetOutput(multiWriter)
}
