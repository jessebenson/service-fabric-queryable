# Basic QueryableService example

BasicApp is a Service Fabric application with several stateful services (Basic.CarSvc, Basic.ProductSvc, Basic.UserSvc).

The products stateful service has an IReliableDictionary<string, Product> collection and an IReliableDictionary<string, Inventory> collection.  It uses the OData Queryable middleware to enable query support.

The users stateful service has an IReliableDictionary<UserName, UserProfile> collection to demonstrate more complex keys and values.  The cars stateful service does not use the Queryable middleware.

# Running the sample

1) Build and deploy the 'BasicApp' Service Fabric application.  The stateful services can be queried using Service Fabric's reverse proxy on port 19081.

2) Perform a GET request for http://localhost:19081/BasicApp/UserSvc/$query/$metadata to see the OData metadata about the 'UserSvc' stateful service.  This will show the reliable collections and their entity types.

3) Perform a GET request for http://localhost:19081/BasicApp/UserSvc/$query/users?$top=5 to see a sample of 5 values from the service.

4) Perform GET requests against http://localhost:19081/BasicApp/UserSvc/$query/users?<OData-query> using the OData query language ($top, $filter, $orderby, $select).
