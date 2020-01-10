package persistence

import (
	"context"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/v3/config"
	cerror "github.com/pip-services3-go/pip-services3-commons-go/v3/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/v3/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/v3/log"
	mcon "github.com/pip-services3-go/pip-services3-mongodb-go/v3/connect"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	mongoclopt "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

/**
MongoDB connection using plain driver.

By defining a connection and sharing it through multiple persistence components
you can reduce number of used database connections.

### Configuration parameters ###

- connection(s):
  - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
  - host:                      host name or IP address
  - port:                      port number (default: 27017)
  - uri:                       resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
  - username:                  (optional) user name
  - password:                  (optional) user password
- options:
  - max_pool_size:             (optional) maximum connection pool size (default: 2)
  - keep_alive:                (optional) enable connection keep alive (default: true)
  - connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
  - socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
  - auto_reconnect:            (optional) enable auto reconnection (default: true)
  - reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000)
  - max_page_size:             (optional) maximum page size (default: 100)
  - replica_set:               (optional) name of replica set
  - ssl:                       (optional) enable SSL connection (default: false)
  - auth_source:               (optional) authentication source
  - debug:                     (optional) enable debug output (default: false).

### References ###

- *:logger:*:*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages
- *:discovery:*:*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
- *:credential-store:*:*:1.0</code> (optional) Credential stores to resolve credentials
*/

//export class MongoDbConnection implements IReferenceable, IConfigurable, IOpenable {
type MongoDbConnection struct {
	defaultConfig cconf.ConfigParams
	//ctx           context.Context
	/*
	   The logger.
	*/
	Logger clog.CompositeLogger
	/*
	   The connection resolver.
	*/
	ConnectionResolver mcon.MongoDbConnectionResolver
	/*
	   The configuration options.
	*/
	Options cconf.ConfigParams
	/*
	   The MongoDB connection object.
	*/
	Connection *mongodrv.Client
	/*
	   The MongoDB database name.
	*/
	DatabaseName string
	/*
	   The MongoDb database object.
	*/
	Db *mongodrv.Database
}

/*
   Creates a new instance of the connection component.
*/
func NewMongoDbConnection() (c *MongoDbConnection) {
	mc := MongoDbConnection{
		defaultConfig: *cconf.NewConfigParamsFromTuples(
			"options.max_pool_size", "2",
			"options.keep_alive", "1000",
			"options.connect_timeout", "5000",
			"options.auto_reconnect", "true",
			"options.max_page_size", "100",
			"options.debug", "true"),
		/*
		 The logger.
		*/
		Logger: *clog.NewCompositeLogger(),
		/*
		 The connection resolver.
		*/
		ConnectionResolver: *mcon.NewMongoDbConnectionResolver(),
		/*
		 The configuration options.
		*/
		Options: *cconf.NewEmptyConfigParams(),
	}

	return &mc
}

/*
   Configures component by passing configuration parameters.
   @param config    configuration parameters to be set.
*/
func (c *MongoDbConnection) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(&c.defaultConfig)

	c.ConnectionResolver.Configure(config)

	c.Options = *c.Options.Override(config.GetSection("options"))
}

/*
 Sets references to dependent components.
 @param references 	references to locate the component dependencies.
*/
func (c *MongoDbConnection) SetReferences(references crefer.IReferences) {
	c.Logger.SetReferences(references)
	c.ConnectionResolver.SetReferences(references)
}

/*
 Checks if the component is opened.
 @returns true if the component has been opened and false otherwise.
*/
func (c *MongoDbConnection) IsOpen() bool {
	return c.Connection != nil
}

func (c *MongoDbConnection) composeSettings() *mongoclopt.ClientOptions {
	var maxPoolSize uint64
	maxPoolSize = (uint64)(c.Options.GetAsInteger("max_pool_size"))
	var MaxConnIdleTime time.Duration
	keepAlive := c.Options.GetAsInteger("keep_alive")
	MaxConnIdleTime = (time.Duration)(keepAlive)
	var ConnectTimeout time.Duration
	connectTimeoutMS := c.Options.GetAsInteger("connect_timeout")
	ConnectTimeout = (time.Duration)(connectTimeoutMS)
	var SocketTimeout time.Duration
	socketTimeoutMS := c.Options.GetAsInteger("socket_timeout")
	SocketTimeout = (time.Duration)(socketTimeoutMS)
	//autoReconnect := c.Options.GetAsNullableBoolean("auto_reconnect");
	//reconnectInterval := c.Options.GetAsNullableInteger("reconnect_interval")
	//debug := c.Options.GetAsNullableBoolean("debug");

	//ssl := c.Options.GetAsNullableBoolean("ssl")
	replicaSet := c.Options.GetAsNullableString("replica_set")
	authSource := c.Options.GetAsString("auth_source")
	authUser := c.Options.GetAsString("auth_user")
	authPassword := c.Options.GetAsString("auth_password")

	settings := mongoclopt.Client()
	settings.MaxPoolSize = &maxPoolSize
	settings.MaxConnIdleTime = &MaxConnIdleTime
	//settings.KeepAlive = keepAlive
	//settings.autoReconnect: autoReconnect
	//settings.ReconnectInterval = reconnectInterval
	settings.ConnectTimeout = &ConnectTimeout
	settings.SocketTimeout = &SocketTimeout

	// if ssl != nil {
	// 	settings.ssl = ssl
	// }
	if replicaSet != nil {
		settings.ReplicaSet = replicaSet
	}

	// Auth params
	var authParams mongoclopt.Credential
	authParams.AuthSource = authSource
	authParams.Username = authUser
	authParams.Password = authPassword
	settings.SetAuth(authParams)

	return settings
}

/*
	 Opens the component.

	 @param correlationId 	(optional) transaction id to trace execution through call chain.
     @param callback 			callback function that receives error or nil no errors occured.
*/
func (c *MongoDbConnection) Open(correlationId string) error {
	uri, err := c.ConnectionResolver.Resolve(correlationId)

	if err != nil {
		c.Logger.Error(correlationId, err, "Failed to resolve MongoDb connection")
		return err
	}

	c.Logger.Debug(correlationId, "Connecting to mongodb")
	// TODO: Need wrote correct settings composer!!!
	//settings := c.composeSettings()
	settings := mongoclopt.Client()
	//settings.useNewUrlParser = true;
	//settings.useUnifiedTopology = true;

	settings.ApplyURI(uri)
	client, err := mongodrv.NewClient(settings)

	if err != nil {
		err = cerror.NewConnectionError(correlationId, "CONNECT_FAILED", "Create client for mongodb failed").WithCause(err)
		return err
	}
	cs, _ := connstring.Parse(uri)
	c.DatabaseName = cs.Database

	// // Todo: change timeout params, must get it from options
	// ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	// c.ctx = ctx
	// defer cancel()
	// err = client.Connect(ctx)
	err = client.Connect(context.TODO())
	if err != nil {
		err = cerror.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to mongodb failed").WithCause(err)
		return err
	}
	c.Connection = client
	c.Db = client.Database(c.DatabaseName)
	//c.DatabaseName = c.Db.Name()
	return nil
}

/*
	 Closes component and frees used resources.

	 @param correlationId 	(optional) transaction id to trace execution through call chain.
     @param callback 			callback function that receives error or nil no errors occured.
*/
func (c *MongoDbConnection) Close(correlationId string) error {
	if c.Connection == nil {
		return nil
	}

	//err := c.Connection.Disconnect(c.ctx)
	err := c.Connection.Disconnect(context.TODO())
	c.Connection = nil
	c.Db = nil
	c.DatabaseName = ""

	if err != nil {
		err = cerror.NewConnectionError(correlationId, "DISCONNECT_FAILED", "Disconnect from mongodb failed: ").WithCause(err)
	} else {
		c.Logger.Debug(correlationId, "Disconnected from mongodb database %s", c.DatabaseName)
	}
	return err
}

func (c *MongoDbConnection) GetConnection() *mongodrv.Client {
	return c.Connection
}

func (c *MongoDbConnection) GetDatabase() *mongodrv.Database {
	return c.Db
}

func (c *MongoDbConnection) GetDatabaseName() string {
	return c.DatabaseName
}
