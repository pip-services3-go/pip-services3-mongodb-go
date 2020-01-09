package test_persistence

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/v3/config"
	cref "github.com/pip-services3-go/pip-services3-commons-go/v3/refer"
	mngpersist "github.com/pip-services3-go/pip-services3-mongodb-go/v3/persistence"
	"os"
	"testing"
)

func TestDummyMongoDbConnection(t *testing.T) {

	var persistence *DummyMongoDbPersistence
	var fixture DummyPersistenceFixture
	var connection *mngpersist.MongoDbConnection

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

	connection = mngpersist.NewMongoDbConnection()
	connection.Configure(dbConfig)

	persistence = NewDummyMongoDbPersistence()
	descr := cref.NewDescriptor("pip-services", "connection", "mongodb", "default", "1.0")
	ref := cref.NewReferencesFromTuples(descr, connection)
	persistence.SetReferences(ref)

	fixture = *NewDummyPersistenceFixture(persistence)

	opnErr := connection.Open("")
	if opnErr != nil {
		t.Error("Error opened connection", opnErr)
		return
	}
	defer connection.Close("")

	opnErr = persistence.Open("")
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

	t.Run("DummyMondoDbConnection:CRUD", fixture.TestCrudOperations)
	t.Run("DummyMondoDbConnection:Batch", fixture.TestBatchOperations)

}
