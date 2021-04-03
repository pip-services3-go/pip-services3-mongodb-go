package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	conn "github.com/pip-services3-go/pip-services3-mongodb-go/connect"
)

//DefaultMongoDbFactory helps creates MongoDb components by their descriptors.
//See Factory
//See MongoDbConnection
type DefaultMongoDbFactory struct {
	cbuild.Factory
}

// NewDefaultMongoDbFactory are create a new instance of the factory.
// Return *DefaultMongoDbFactory
func NewDefaultMongoDbFactory() *DefaultMongoDbFactory {
	c := DefaultMongoDbFactory{}

	mongoDbConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "mongodb", "*", "1.0")

	c.RegisterType(mongoDbConnectionDescriptor, persist.NewMongoDbConnection)
	return &c
}
