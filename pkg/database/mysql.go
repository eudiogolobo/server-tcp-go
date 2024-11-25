package database

import (
    "database/sql"
    "fmt"
    _ "github.com/go-sql-driver/mysql" // Importando o driver MySQL
)

// Função para verificar se um dispositivo existe na tabela 'devices' e retornar o id da tabela
func GetDeviceID(db *sql.DB, deviceID string) (int64, error) {
    // Verifica se a conexão com o banco ainda é válida
    if err := db.Ping(); err != nil {
        return 0, fmt.Errorf("erro ao verificar conexão com o banco de dados: %v", err)
    }

    query := "SELECT id FROM devices WHERE device_id = ? AND status = 1 LIMIT 1"

    // Executa a consulta e armazena o id encontrado
    var id int64
    err := db.QueryRow(query, deviceID).Scan(&id)
    if err != nil {
        if err == sql.ErrNoRows {
            // Se não encontrar nenhum dispositivo, retorna um erro de dispositivo não encontrado
            return 0, nil
        }
        return 0, fmt.Errorf("erro ao executar consulta para verificar dispositivo: %v", err)
    }

    // Retorna o id encontrado
    return id, nil
}


