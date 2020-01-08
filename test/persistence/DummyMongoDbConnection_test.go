package test_persistence

// import (
// 	"testing"
// )

// func TestDummyMemoryPersistence(t *testing.T) {

// 	let connection: MongoDbConnection;
//     let persistence: DummyMongoDbPersistence;
//     let fixture: DummyPersistenceFixture;

//     let mongoUri = process.env['MONGO_URI'];
//     let mongoHost = process.env['MONGO_HOST'] || 'localhost';
//     let mongoPort = process.env['MONGO_PORT'] || 27017;
//     let mongoDatabase = process.env['MONGO_DB'] || 'test';
//     if (mongoUri == null && mongoHost == null)
// 		return;

// 		let dbConfig = ConfigParams.fromTuples(
//             'connection.uri', mongoUri,
//             'connection.host', mongoHost,
//             'connection.port', mongoPort,
//             'connection.database', mongoDatabase
//         );

//         connection = new MongoDbConnection();
//         connection.configure(dbConfig);

//         persistence = new DummyMongoDbPersistence();
//         persistence.setReferences(References.fromTuples(
//             new Descriptor("pip-services", "connection", "mongodb", "default", "1.0"), connection
//         ));

//         fixture = new DummyPersistenceFixture(persistence);

//         connection.open(null, (err: any) => {
//             if (err) {
//                 done(err);
//                 return;
//             }

//             persistence.open(null, (err: any) => {
//                 if (err) {
//                     done(err);
//                     return;
//                 }

//                 persistence.clear(null, (err) => {
//                     done(err);
//                 });
//             });
// 		});

// 		////////////////////////////

// 		test('Crud Operations', (done) => {
// 			fixture.testCrudOperations(done);
// 		});

// 		test('Batch Operations', (done) => {
// 			fixture.testBatchOperations(done);
// 		});

// 		////////////////////////////

// 		connection.close(null, (err) => {
//             persistence.close(null, done);
//         });

// 	// t.Run("DummyMemoryPersistence:CRUD", fixture.TestCrudOperations)
// 	// t.Run("DummyMemoryPersistence:Batch", fixture.TestBatchOperations)

// }
