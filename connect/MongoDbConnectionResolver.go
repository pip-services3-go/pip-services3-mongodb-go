package connect

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/data"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/refer"
	"github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
	"strconv"
	"sync"
)

/*
MongoDbConnectionResolver a helper struct  that resolves MongoDB connection and credential parameters,
validates them and generates a connection URI.
It is able to process multiple connections to MongoDB cluster nodes.

Configuration parameters

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from IDiscovery
  - host:                        host name or IP address
  - port:                        port number (default: 27017)
  - database:                    database name
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from ICredentialStore
  - username:                    user name
  - password:                    user password

 References

- *:discovery:*:*:1.0             (optional) IDiscovery services
- *:credential-store:*:*:1.0      (optional) Credential stores to resolve credentials
*/
type MongoDbConnectionResolver struct {
	//The connections resolver.
	ConnectionResolver ccon.ConnectionResolver
	//The credentials resolver.
	CredentialResolver auth.CredentialResolver
}

// NewMongoDbConnectionResolver creates new connection resolver
// Retruns *MongoDbConnectionResolver
func NewMongoDbConnectionResolver() *MongoDbConnectionResolver {
	mongoCon := MongoDbConnectionResolver{}
	mongoCon.ConnectionResolver = *ccon.NewEmptyConnectionResolver()
	mongoCon.CredentialResolver = *auth.NewEmptyCredentialResolver()
	return &mongoCon
}

// Configure is configures component by passing configuration parameters.
// Parameters:
// 	- config  *cconf.ConfigParams
//  configuration parameters to be set.
func (c *MongoDbConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

// SetReferences is sets references to dependent components.
// Parameters:
// 	- references crefer.IReferences
//	references to locate the component dependencies.
func (c *MongoDbConnectionResolver) SetReferences(references crefer.IReferences) {
	c.ConnectionResolver.SetReferences(references)
	c.CredentialResolver.SetReferences(references)
}

func (c *MongoDbConnectionResolver) validateConnection(correlationId string, connection *ccon.ConnectionParams) error {
	uri := connection.Uri()
	if uri != "" {
		return nil
	}

	host := connection.Host()
	if host == "" {
		return cerr.NewConfigError(correlationId, "NO_HOST", "Connection host is not set")
	}
	port := connection.Port()
	if port == 0 {
		return cerr.NewConfigError(correlationId, "NO_PORT", "Connection port is not set")
	}
	database := connection.GetAsNullableString("database")
	if *database == "" {
		return cerr.NewConfigError(correlationId, "NO_DATABASE", "Connection database is not set")
	}
	return nil
}

func (c *MongoDbConnectionResolver) validateConnections(correlationId string, connections []*ccon.ConnectionParams) error {
	if connections == nil || len(connections) == 0 {
		return cerr.NewConfigError(correlationId, "NO_CONNECTION", "Database connection is not set")
	}
	for _, connection := range connections {
		err := c.validateConnection(correlationId, connection)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *MongoDbConnectionResolver) composeUri(connections []*ccon.ConnectionParams, credential *auth.CredentialParams) string {
	// If there is a uri then return it immediately
	for _, connection := range connections {
		uri := connection.Uri()
		if uri != "" {
			return uri
		}
	}

	// Define hosts
	var hosts = ""
	for _, connection := range connections {
		host := connection.Host()
		port := connection.Port()

		if len(hosts) > 0 {
			hosts += ","
		}
		if port != 0 {
			hosts += host + ":" + strconv.Itoa(port)
		}

	}

	// Define database
	database := ""
	for _, connection := range connections {
		if database == "" {
			database = *connection.GetAsNullableString("database")
		}
	}
	if len(database) > 0 {
		database = "/" + database
	}

	// Define authentication part
	var auth = ""
	if credential != nil {
		var username = credential.Username()
		if len(username) > 0 {
			var password = credential.Password()
			if len(password) > 0 {
				auth = username + ":" + password + "@"
			} else {
				auth = username + "@"
			}
		}
	}
	// Define additional parameters
	consConf := cdata.NewEmptyStringValueMap()
	for _, v := range connections {
		consConf.Append(v.Value())
	}
	var options *cconf.ConfigParams
	if credential != nil {
		options = cconf.NewConfigParamsFromMaps(consConf.Value(), credential.Value())
	} else {
		options = cconf.NewConfigParamsFromValue(consConf.Value())
	}
	options.Remove("uri")
	options.Remove("host")
	options.Remove("port")
	options.Remove("database")
	options.Remove("username")
	options.Remove("password")
	params := ""
	keys := options.Keys()
	for _, key := range keys {
		if len(params) > 0 {
			params += "&"
		}
		params += key

		value := options.GetAsString(key)
		if value != "" {
			params += "=" + value
		}
	}
	if len(params) > 0 {
		params = "?" + params
	}

	// Compose uri
	uri := "mongodb://" + auth + hosts + database + params

	return uri
}

// Resolve method are resolves MongoDB connection URI from connection and credential parameters.
// Parameters:
// 	- correlationId  string
//	(optional) transaction id to trace execution through call chain.
// Returns uri string, err error
// resolved URI and error, if this occured.
func (c *MongoDbConnectionResolver) Resolve(correlationId string) (uri string, err error) {
	var connections []*ccon.ConnectionParams
	var credential *auth.CredentialParams
	var errCred, errConn error

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		connections, errConn = c.ConnectionResolver.ResolveAll(correlationId)
		//Validate connections
		if errConn == nil {
			errConn = c.validateConnections(correlationId, connections)
		}
	}()
	go func() {
		defer wg.Done()
		credential, errCred = c.CredentialResolver.Lookup(correlationId)
		// Credentials are not validated right now
	}()
	wg.Wait()

	if errConn != nil {
		return uri, errConn
	}
	if errCred != nil {
		return uri, errCred
	}
	uri = c.composeUri(connections, credential)
	return uri, nil
}
