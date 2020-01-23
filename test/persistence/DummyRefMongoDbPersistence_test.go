package test_persistence

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
)

func TestDummyRefMongoDbPersistence(t *testing.T) {

	var persistence *DummyRefMongoDbPersistence
	var fixture DummyRefPersistenceFixture

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

	persistence = NewDummyRefMongoDbPersistence()
	persistence.Configure(dbConfig)

	fixture = *NewDummyRefPersistenceFixture(persistence)

	opnErr := persistence.Open("")
	if opnErr != nil {
		t.Error("Error opened persistence", opnErr)
		return
	}
	defer persistence.Close("")

	opnErr = persistence.Clear("")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummyRefMongoDbPersistence:CRUD", fixture.TestCrudOperations)
	t.Run("DummyRefMongoDbPersistence:Batch", fixture.TestBatchOperations)

}
