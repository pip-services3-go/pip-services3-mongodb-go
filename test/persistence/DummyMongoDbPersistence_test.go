package test_persistence

// import (
// 	"testing"
// )

// func TestDummyMemoryPersistence(t *testing.T) {

// let persistence: DummyMongoDbPersistence;
// let fixture: DummyPersistenceFixture;

// let mongoUri = process.env['MONGO_URI'];
// let mongoHost = process.env['MONGO_HOST'] || 'localhost';
// let mongoPort = process.env['MONGO_PORT'] || 27017;
// let mongoDatabase = process.env['MONGO_DB'] || 'test';
// if (mongoUri == null && mongoHost == null)
//     return;

// setup((done) => {
//     let dbConfig = ConfigParams.fromTuples(
//         'connection.uri', mongoUri,
//         'connection.host', mongoHost,
//         'connection.port', mongoPort,
//         'connection.database', mongoDatabase
//     );

//     persistence = new DummyMongoDbPersistence();
//     persistence.configure(dbConfig);

//     fixture = new DummyPersistenceFixture(persistence);

//     persistence.open(null, (err: any) => {
//         if (err) {
//             done(err);
//             return;
//         }

//         persistence.clear(null, (err) => {
//             done(err);
//         });
//     });
// });

// teardown((done) => {
//     persistence.close(null, done);
// });

// test('Crud Operations', (done) => {
//     fixture.testCrudOperations(done);
// });

// test('Batch Operations', (done) => {
//     fixture.testBatchOperations(done);
// });

// 	// persister := NewDummyMemoryPersistence()
// 	// persister.Configure(*cconf.NewEmptyConfigParams())

// 	// fixture := NewDummyPersistenceFixture(persister)

// 	// t.Run("DummyMemoryPersistence:CRUD", fixture.TestCrudOperations)
// 	// t.Run("DummyMemoryPersistence:Batch", fixture.TestBatchOperations)

// }
