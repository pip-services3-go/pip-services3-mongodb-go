package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	cmngpersist "github.com/pip-services3-go/pip-services3-mongodb-go/persistence"
)

//DefaultMongoDbFactory helps creates MongoDb components by their descriptors.
//See Factory
//See MongoDbConnection
type DefaultMongoDbFactory struct {
	cbuild.Factory
	Descriptor                  *cref.Descriptor
	MongoDbConnectionDescriptor *cref.Descriptor
}

// NewDefaultMongoDbFactory are create a new instance of the factory.
// Return *DefaultMongoDbFactory
func NewDefaultMongoDbFactory() *DefaultMongoDbFactory {
	mongoDBFactory := DefaultMongoDbFactory{}
	mongoDBFactory.Descriptor = cref.NewDescriptor("pip-services", "factory", "rpc", "default", "1.0")
	mongoDBFactory.MongoDbConnectionDescriptor = cref.NewDescriptor("pip-services", "connection", "mongodb", "*", "1.0")
	mongoDBFactory.RegisterType(mongoDBFactory.MongoDbConnectionDescriptor, cmngpersist.NewMongoDbConnection)
	return &mongoDBFactory
}
