# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> MongoDB components for Golang Changelog

## <a name="1.1.4"></a> 1.1.4 (2023-01-12) 
- Update dependencies

## <a name="1.1.2"></a> 1.1.2 (2022-01-25) 
### Bug Fixes
- Fixed MongoDbPersistence method in GetOneRandom.
- Fixed database cursor closing after operations.
- Fixed DefaultMongoDbFactory registration.

## <a name="1.1.1"></a> 1.1.1 (2022-01-19) 
### Bug Fixes
- Fix MongoDbPersistence method in GetListByFilter.

## <a name="1.1.0"></a> 1.1.0 (2021-04-03) 

### Breaking changes
* Moved MongoDbConnection to connect package
* Added IMongoDbPersistenceOverrides to persistence constructors to support virtual methods

## <a name="1.0.5"></a> 1.0.5 (2020-12-11) 

### Features
*  Update dependencies

## <a name="1.0.4"></a> 1.0.4 (2020-08-05) 

### Features
*  Rafactoring code

## <a name="1.0.3"></a> 1.0.3 (2020-08-04) 

### Features
*  Fix returns data in GetPageByFilter method

## <a name="1.0.2"></a> 1.0.2 (2020-07-12) 

### Features
* Moved some CRUD operations from IdentifiableMongoDbPersistence to MongoDbPersistence

## <a name="1.0.1"></a> 1.0.1 (2020-05-19) 

### Features
* Added GetCountByFilter method in IdentifiebleMongodbPersistence

## <a name="1.0.0"></a> 1.0.0 (2020-03-05)

Initial public release

### Features
* **build** factory for creating MongoDB persistence components
* **connect** instruments for configuring connections to the database
* **persistence** abstract classes for working with the database
