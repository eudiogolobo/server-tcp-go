package config

import (
    "database/sql"
    "fmt"
    "time"
    _ "github.com/go-sql-driver/mysql" // Importando o driver MySQL
    "github.com/joho/godotenv"
    "log"
    "os"                               // Adicionando a importação do pacote os

)

// Função para conectar ao banco de dados MySQL
func ConnectToDatabase() (*sql.DB, error) {

      // Carregar as variáveis de ambiente do arquivo .env
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Erro ao carregar arquivo .env: %v", err)
	}

    // Acessando variáveis de ambiente
	databaseUrl := os.Getenv("DATABASE_URL")

	if databaseUrl == "" {
		// Valor padrão caso a variável de ambiente não esteja definida
		databaseUrl = "root:@tcp(localhost:3306)/satnew"
	}

    dsn := databaseUrl // Substitua pelas suas credenciais
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return nil, fmt.Errorf("erro ao conectar ao banco de dados: %v", err)
    }

    // Testa a conexão com o banco
    if err := db.Ping(); err != nil {
        return nil, fmt.Errorf("erro ao conectar ao banco de dados: %v", err)
    }

    fmt.Println("Conexão com o banco de dados estabelecida com sucesso!")

    // Configurações do pool de conexões
    db.SetMaxOpenConns(100)    // Número máximo de conexões simultâneas abertas
    db.SetMaxIdleConns(10)     // Número máximo de conexões ociosas
    db.SetConnMaxLifetime(5 * time.Minute)  // Tempo máximo de vida útil de uma conexão

    return db, nil
}
