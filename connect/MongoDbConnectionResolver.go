package connect

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/v3/config"
	cdata "github.com/pip-services3-go/pip-services3-commons-go/v3/data"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/v3/errors"
	crefer "github.com/pip-services3-go/pip-services3-commons-go/v3/refer"
	"github.com/pip-services3-go/pip-services3-components-go/v3/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/v3/connect"
	"strconv"
	"sync"
)

/*
Helper class that resolves MongoDB connection and credential parameters,
validates them and generates a connection URI.

It is able to process multiple connections to MongoDB cluster nodes.

### Configuration parameters ###

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
  - host:                        host name or IP address
  - port:                        port number (default: 27017)
  - database:                    database name
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
  - username:                    user name
  - password:                    user password

### References ###

- <code>\*:discovery:\*:\*:1.0</code>             (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
- <code>\*:credential-store:\*:\*:1.0</code>      (optional) Credential stores to resolve credentials
*/

//implements IReferenceable, IConfigurable
type MongoDbConnectionResolver struct {
	/*
	   The connections resolver.
	*/
	ConnectionResolver ccon.ConnectionResolver
	/*
	   The credentials resolver.
	*/
	CredentialResolver auth.CredentialResolver
}

func NewMongoDbConnectionResolver() *MongoDbConnectionResolver {
	mongoCon := MongoDbConnectionResolver{}
	mongoCon.ConnectionResolver = *ccon.NewEmptyConnectionResolver()
	mongoCon.CredentialResolver = *auth.NewEmptyCredentialResolver()
	return &mongoCon
}

/*
   Configures component by passing configuration parameters.

   @param config    configuration parameters to be set.
*/
func (c *MongoDbConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

/*
	Sets references to dependent components.

	@param references 	references to locate the component dependencies.
*/
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

/*
   Resolves MongoDB connection URI from connection and credential parameters.

   @param correlationId     (optional) transaction id to trace execution through call chain.
   @param callback 			callback function that receives resolved URI or error.
*/
func (c *MongoDbConnectionResolver) Resolve(correlationId string) (uri string, err error) {
	var connections []*ccon.ConnectionParams
	var credential *auth.CredentialParams
	var errCred, errConn error

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		connections, errConn = c.ConnectionResolver.ResolveAll(correlationId)
		//copy(connections, result)
		//Validate connections
		if errConn == nil {
			errConn = c.validateConnections(correlationId, connections)
		}
	}()
	go func() {
		defer wg.Done()
		credential, errCred = c.CredentialResolver.Lookup(correlationId)
		// if errCred == nil {
		// 	credential = result
		// }
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
