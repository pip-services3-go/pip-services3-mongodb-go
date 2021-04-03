package test_connect

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	conn "github.com/pip-services3-go/pip-services3-mongodb-go/connect"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestMongoDBConnection(t *testing.T) {
	var connection conn.MongoDbConnection

	mongoUri := os.Getenv("MONGO_URI")
	mongoHost := os.Getenv("MONGO_HOST")
	if mongoHost == "" {
		mongoHost = "localhost"
	}
	mongoPort := os.Getenv("MONGO_PORT")
	if mongoPort == "" {
		mongoPort = "27017"
	}
	mongoDatabase := os.Getenv("MONGO_DB")
	if mongoDatabase == "" {
		mongoDatabase = "test"
	}
	if mongoUri == "" && mongoHost == "" {
		return
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.uri", mongoUri,
		"connection.host", mongoHost,
		"connection.port", mongoPort,
		"connection.database", mongoDatabase,
	)

	connection = *conn.NewMongoDbConnection()
	connection.Configure(dbConfig)

	connection.Open("")

	defer connection.Close("")

	//test("Open and Close")
	assert.NotNil(t, connection.GetConnection())
	assert.NotNil(t, connection.GetDatabase())
	assert.NotEqual(t, "", connection.GetDatabaseName())

}
